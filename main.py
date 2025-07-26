import logging
import signal
import ssl
import threading
import time
from typing import Any, Dict, List
import json
import pika
import psutil
import os
from clickhouse_driver import Client
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from datetime import datetime

import config

# ----------------------------- #
#         CONFIGURATION         #
# ----------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(config.LOG_PATH), logging.StreamHandler()],
)

# ----------------------------- #
#          PROMETHEUS           #
# ----------------------------- #
# Общие метрики обработки сообщений
TOTAL_MESSAGES = Counter(
    "radius_messages_total",
    "Total processed messages",
    [
        "queue",
        "status",
    ],  # status: success, validation_error, transient_error, critical_error
)

PROCESS_DURATION = Histogram(
    "radius_process_duration_seconds",
    "Time spent processing a message",
    ["queue", "operation"],  # operation: validation, insertion, batch_flush
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# Метрики валидации
VALIDATION_ERRORS = Counter(
    "radius_validation_errors_total",
    "Total validation errors by field",
    ["queue", "field", "error_type"],
)

# Метрики ClickHouse
CLICKHOUSE_OPERATIONS = Counter(
    "radius_clickhouse_operations_total",
    "Total ClickHouse operations",
    ["table", "operation", "status"],  # operation: insert, batch_insert
)

CLICKHOUSE_DURATION = Histogram(
    "radius_clickhouse_operation_duration_seconds",
    "ClickHouse operation duration",
    ["table", "operation"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

CLICKHOUSE_BATCH_SIZE = Histogram(
    "radius_clickhouse_batch_size",
    "Size of batches sent to ClickHouse",
    ["table"],
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)

# Метрики RabbitMQ
RABBITMQ_CONNECTIONS = Counter(
    "radius_rabbitmq_connections_total",
    "Total RabbitMQ connection attempts",
    ["queue", "status"],  # status: success, failed, reconnect
)

QUEUE_DEPTH = Gauge("radius_queue_depth", "Current queue depth", ["queue"])

# Метрики здоровья системы
SYSTEM_HEALTH = Gauge(
    "radius_system_health",
    "System health status (1=healthy, 0=unhealthy)",
    ["component"],  # component: clickhouse, rabbitmq, validation
)

# Метрики производительности
ACTIVE_CONNECTIONS = Gauge(
    "radius_active_connections", "Number of active connections", ["type"]
)

BATCH_PROCESSING_TIME = Histogram(
    "radius_batch_processing_seconds",
    "Time to process a batch of messages",
    ["queue"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)

MEMORY_USAGE = Gauge("radius_memory_usage_bytes", "Memory usage in bytes")


# ----------------------------- #
#         CLICKHOUSE CLIENT     #
# ----------------------------- #
class ClickHouseClient:
    def __init__(self) -> None:
        self.client = Client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
        )
        # Устанавливаем начальное состояние здоровья
        SYSTEM_HEALTH.labels(component="clickhouse").set(1)

    def insert_session(self, data: Dict[str, Any]) -> None:
        """Insert single session into ClickHouse (data already validated)."""
        try:
            # Валидация и подготовка данных
            with PROCESS_DURATION.labels(
                queue="session_queue", operation="validation"
            ).time():
                validated_data = DataValidator.validate_session_data(data)
                row = prepare_session_row(validated_data)

            # Вставка в ClickHouse
            with CLICKHOUSE_DURATION.labels(
                table="radius_sessions_new", operation="insert"
            ).time():
                self.client.execute(
                    "INSERT INTO radius.radius_sessions_new (*) VALUES",
                    [row],
                )

            # Метрики успеха
            CLICKHOUSE_OPERATIONS.labels(
                table="radius_sessions_new", operation="insert", status="success"
            ).inc()

            SYSTEM_HEALTH.labels(component="clickhouse").set(1)

        except Exception as e:
            # Логируем критическую ошибку
            logging.error(f"ClickHouse session insert error: {e}")
            CLICKHOUSE_OPERATIONS.labels(
                table="radius_sessions_new", operation="insert", status="error"
            ).inc()
            SYSTEM_HEALTH.labels(component="clickhouse").set(0)
            raise

    def insert_traffic_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Insert traffic batch into ClickHouse (data already validated)."""
        if not batch:
            return

        try:
            # Подготовка данных для вставки
            with PROCESS_DURATION.labels(
                queue="traffic_queue", operation="preparation"
            ).time():
                validated_rows = []
                for item in batch:
                    validated_data = DataValidator.validate_traffic_data(item)
                    validated_rows.append(prepare_traffic_row(validated_data))

            # Вставка батча в ClickHouse
            with CLICKHOUSE_DURATION.labels(
                table="radius_traffic", operation="batch_insert"
            ).time():
                self.client.execute(
                    "INSERT INTO radius.radius_traffic (*) VALUES",
                    validated_rows,
                )

            # Метрики успеха
            CLICKHOUSE_OPERATIONS.labels(
                table="radius_traffic", operation="batch_insert", status="success"
            ).inc()

            CLICKHOUSE_BATCH_SIZE.labels(table="radius_traffic").observe(len(batch))
            SYSTEM_HEALTH.labels(component="clickhouse").set(1)

        except Exception as e:
            # Логируем критическую ошибку
            logging.error(f"ClickHouse traffic batch insert error: {e}")
            CLICKHOUSE_OPERATIONS.labels(
                table="radius_traffic", operation="batch_insert", status="error"
            ).inc()
            SYSTEM_HEALTH.labels(component="clickhouse").set(0)
            raise


# ----------------------------- #
#         VALIDATION / PREP     #
# ----------------------------- #


class ValidationError(Exception):
    """Кастомное исключение для ошибок валидации"""

    def __init__(self, field: str, value: Any, message: str):
        self.field = field
        self.value = value
        self.message = message
        super().__init__(f"Field '{field}': {message} (value: {value})")


class DataValidator:
    """Валидатор данных для сессий и трафика"""

    # Поля таблицы radius_sessions_new согласно схеме
    SESSION_REQUIRED_FIELDS = {"Acct-Unique-Session-Id", "Acct-Start-Time"}

    SESSION_STRING_FIELDS = {
        "login",
        "onu_mac",
        "contract",
        "auth_type",
        "service",
        "Acct-Session-Id",
        "Acct-Unique-Session-Id",
        "User-Name",
        "NAS-IP-Address",
        "NAS-Port-Id",
        "NAS-Port-Type",
        "Calling-Station-Id",
        "Acct-Terminate-Cause",
        "Service-Type",
        "Framed-Protocol",
        "Framed-IP-Address",
        "Framed-IPv6-Prefix",
        "Delegated-IPv6-Prefix",
        "ERX-Virtual-Router-Name",
        "ERX-Service-Session",
        "ADSL-Agent-Circuit-Id",
        "ADSL-Agent-Remote-Id",
    }

    SESSION_DATETIME_FIELDS = {"Acct-Start-Time", "Acct-Update-Time", "Acct-Stop-Time"}

    SESSION_UINT32_FIELDS = {"Acct-Session-Time"}

    SESSION_UINT64_FIELDS = {
        "Acct-Input-Octets",
        "Acct-Output-Octets",
        "Acct-Input-Packets",
        "Acct-Output-Packets",
        "Acct-Input-Gigawords",
        "Acct-Output-Gigawords",
        "ERX-IPv6-Acct-Input-Octets",
        "ERX-IPv6-Acct-Output-Octets",
        "ERX-IPv6-Acct-Input-Packets",
        "ERX-IPv6-Acct-Output-Packets",
        "ERX-IPv6-Acct-Input-Gigawords",
        "ERX-IPv6-Acct-Output-Gigawords",
    }

    SESSION_INT8_FIELDS = {"GMT"}

    # Поля таблицы radius_traffic согласно схеме
    TRAFFIC_REQUIRED_FIELDS = {"Acct-Unique-Session-Id"}

    TRAFFIC_STRING_FIELDS = {"Acct-Unique-Session-Id", "login"}

    TRAFFIC_DATETIME_FIELDS = {"timestamp"}

    TRAFFIC_UINT64_FIELDS = {
        "Acct-Input-Octets",
        "Acct-Output-Octets",
        "Acct-Input-Packets",
        "Acct-Output-Packets",
        "ERX-IPv6-Acct-Input-Octets",
        "ERX-IPv6-Acct-Output-Octets",
        "ERX-IPv6-Acct-Input-Packets",
        "ERX-IPv6-Acct-Output-Packets",
    }

    @staticmethod
    def validate_session_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Валидация данных сессии"""
        if not isinstance(data, dict):
            raise ValidationError("root", data, "Data must be a dictionary")

        # Проверка обязательных полей
        for field in DataValidator.SESSION_REQUIRED_FIELDS:
            if field not in data or data[field] is None or data[field] == "":
                raise ValidationError(
                    field, data.get(field), "Required field is missing or empty"
                )

        validated_data = {}

        # Валидация строковых полей
        for field in DataValidator.SESSION_STRING_FIELDS:
            if field in data:
                value = data[field]
                if value is not None:
                    validated_data[field] = str(value)[:255]  # Ограничиваем длину
                else:
                    validated_data[field] = ""

        # Валидация DateTime полей
        for field in DataValidator.SESSION_DATETIME_FIELDS:
            if field in data:
                validated_data[field] = DataValidator._parse_datetime(
                    field, data[field]
                )

        # Валидация UInt32 полей
        for field in DataValidator.SESSION_UINT32_FIELDS:
            if field in data:
                validated_data[field] = DataValidator._parse_uint32(field, data[field])

        # Валидация UInt64 полей
        for field in DataValidator.SESSION_UINT64_FIELDS:
            if field in data:
                validated_data[field] = DataValidator._parse_uint64(field, data[field])

        # Валидация Int8 полей
        for field in DataValidator.SESSION_INT8_FIELDS:
            if field in data:
                validated_data[field] = DataValidator._parse_int8(field, data[field])
            else:
                validated_data[field] = 5  # DEFAULT значение для GMT

        return validated_data

    @staticmethod
    def validate_traffic_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Валидация данных трафика"""
        if not isinstance(data, dict):
            raise ValidationError("root", data, "Data must be a dictionary")

        # Проверка обязательных полей
        for field in DataValidator.TRAFFIC_REQUIRED_FIELDS:
            if field not in data or data[field] is None or data[field] == "":
                raise ValidationError(
                    field, data.get(field), "Required field is missing or empty"
                )

        validated_data = {}

        # Валидация строковых полей
        for field in DataValidator.TRAFFIC_STRING_FIELDS:
            if field in data:
                value = data[field]
                if value is not None:
                    validated_data[field] = str(value)[:255]
                else:
                    raise ValidationError(field, value, "String field cannot be None")

        # Валидация DateTime полей
        for field in DataValidator.TRAFFIC_DATETIME_FIELDS:
            if field in data:
                validated_data[field] = DataValidator._parse_datetime(
                    field, data[field]
                )

        # Валидация UInt64 полей
        for field in DataValidator.TRAFFIC_UINT64_FIELDS:
            if field in data:
                validated_data[field] = DataValidator._parse_uint64(field, data[field])
            else:
                validated_data[field] = 0  # DEFAULT значение

        return validated_data

    @staticmethod
    def _parse_datetime(field: str, value: Any) -> datetime:
        """Парсинг DateTime значения"""
        if value is None or value == "":
            return datetime(1970, 1, 1, 5, 0, 0)

        if isinstance(value, datetime):
            return value

        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value)
            except (ValueError, OSError) as e:
                raise ValidationError(field, value, f"Invalid timestamp: {e}")

        if isinstance(value, str):
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            raise ValidationError(field, value, "Invalid datetime format")

        raise ValidationError(field, value, "Cannot convert to datetime")

    @staticmethod
    def _parse_uint32(field: str, value: Any) -> int:
        """Парсинг UInt32 значения"""
        if value is None or value == "":
            return 0

        try:
            result = int(float(value))
            if result < 0:
                raise ValidationError(field, value, "UInt32 cannot be negative")
            if result > 4294967295:  # 2^32 - 1
                raise ValidationError(field, value, "UInt32 overflow")
            return result
        except (ValueError, TypeError) as e:
            raise ValidationError(field, value, f"Cannot convert to UInt32: {e}")

    @staticmethod
    def _parse_uint64(field: str, value: Any) -> int:
        """Парсинг UInt64 значения"""
        if value is None or value == "":
            return 0

        try:
            result = int(float(value))
            if result < 0:
                raise ValidationError(field, value, "UInt64 cannot be negative")
            if result > 18446744073709551615:  # 2^64 - 1
                raise ValidationError(field, value, "UInt64 overflow")
            return result
        except (ValueError, TypeError) as e:
            raise ValidationError(field, value, f"Cannot convert to UInt64: {e}")

    @staticmethod
    def _parse_int8(field: str, value: Any) -> int:
        """Парсинг Int8 значения"""
        if value is None or value == "":
            return 5  # DEFAULT для GMT

        try:
            result = int(float(value))
            if result < -128 or result > 127:
                raise ValidationError(field, value, "Int8 overflow")
            return result
        except (ValueError, TypeError) as e:
            raise ValidationError(field, value, f"Cannot convert to Int8: {e}")


# Все поля для трафика
TRAFFIC_FIELDS = list(
    DataValidator.TRAFFIC_STRING_FIELDS
    | DataValidator.TRAFFIC_DATETIME_FIELDS
    | DataValidator.TRAFFIC_UINT64_FIELDS
)

SESSION_FIELDS = [
    "login",
    "onu_mac",
    "contract",
    "auth_type",
    "service",
    "Acct-Session-Id",
    "Acct-Unique-Session-Id",
    "Acct-Start-Time",
    "Acct-Update-Time",
    "Acct-Stop-Time",
    "User-Name",
    "NAS-IP-Address",
    "NAS-Port-Id",
    "NAS-Port-Type",
    "Calling-Station-Id",
    "Acct-Terminate-Cause",
    "Service-Type",
    "Framed-Protocol",
    "Framed-IP-Address",
    "Framed-IPv6-Prefix",
    "Delegated-IPv6-Prefix",
    "Acct-Session-Time",
    "Acct-Input-Octets",
    "Acct-Output-Octets",
    "Acct-Input-Packets",
    "Acct-Output-Packets",
    "Acct-Input-Gigawords",
    "Acct-Output-Gigawords",
    "ERX-IPv6-Acct-Input-Octets",
    "ERX-IPv6-Acct-Output-Octets",
    "ERX-IPv6-Acct-Input-Packets",
    "ERX-IPv6-Acct-Output-Packets",
    "ERX-IPv6-Acct-Input-Gigawords",
    "ERX-IPv6-Acct-Output-Gigawords",
    "ERX-Virtual-Router-Name",
    "ERX-Service-Session",
    "ADSL-Agent-Circuit-Id",
    "ADSL-Agent-Remote-Id",
    "GMT",
]


def prepare_session_row(validated_data: Dict[str, Any]) -> List[Any]:
    """Подготовка строки для вставки в таблицу sessions"""
    # Проверка обязательных полей
    if not all(
        field in validated_data for field in DataValidator.SESSION_REQUIRED_FIELDS
    ):
        missing = [
            f for f in DataValidator.SESSION_REQUIRED_FIELDS if f not in validated_data
        ]
        raise ValueError(f"Missing required fields: {missing}")

    row = []
    for field in SESSION_FIELDS:
        value = validated_data.get(field)

        # Обработка отсутствующих значений
        if value is None:
            if field in DataValidator.SESSION_DATETIME_FIELDS:
                row.append(datetime(1970, 1, 1, 5, 0, 0))
            elif field in DataValidator.SESSION_UINT32_FIELDS:
                row.append(0)
            elif field in DataValidator.SESSION_UINT64_FIELDS:
                row.append(0)
            elif field in DataValidator.SESSION_INT8_FIELDS:
                row.append(5 if field == "GMT" else 0)
            elif field in DataValidator.SESSION_STRING_FIELDS:
                row.append("")
            else:
                row.append(None)
            continue

        # Явное преобразование типов
        try:
            if field in DataValidator.SESSION_STRING_FIELDS:
                row.append(str(value))
            elif field in DataValidator.SESSION_DATETIME_FIELDS:
                if not isinstance(value, datetime):
                    raise ValueError(f"Field {field} must be datetime")
                row.append(value)
            elif field in DataValidator.SESSION_UINT32_FIELDS:
                row.append(int(value))
            elif field in DataValidator.SESSION_UINT64_FIELDS:
                row.append(int(value))
            elif field in DataValidator.SESSION_INT8_FIELDS:
                row.append(int(value))
            else:
                row.append(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid value for field {field}: {value} ({type(value)}). Error: {str(e)}"
            )

    return row


def prepare_traffic_row(validated_data: Dict[str, Any]) -> List[Any]:
    """Подготовка строки для вставки в таблицу traffic"""
    return [
        validated_data["Acct-Unique-Session-Id"],
        validated_data["login"],
        validated_data["Acct-Input-Octets"],
        validated_data["Acct-Output-Octets"],
        validated_data["Acct-Input-Packets"],
        validated_data["Acct-Output-Packets"],
        validated_data["ERX-IPv6-Acct-Input-Octets"],
        validated_data["ERX-IPv6-Acct-Output-Octets"],
        validated_data["ERX-IPv6-Acct-Input-Packets"],
        validated_data["ERX-IPv6-Acct-Output-Packets"],
        validated_data["timestamp"],
    ]


# ----------------------------- #
#         RABBITMQ BASE         #
# ----------------------------- #
class RabbitConsumer:
    def __init__(self, queue: str) -> None:
        self.queue = queue
        self.conn = None
        self.channel = None
        self.running = False
        self.ssl_ctx = None

        if config.CA_CERT_PATH:
            self.ssl_ctx = ssl.create_default_context(cafile=config.CA_CERT_PATH)
            self.ssl_ctx.load_cert_chain(
                certfile=config.CLIENT_CERT_PATH,
                keyfile=config.CLIENT_KEY_PATH,
            )

    def connect(self) -> None:
        params = pika.URLParameters(config.AMQP_URL)
        if self.ssl_ctx:
            params.ssl_options = pika.SSLOptions(self.ssl_ctx)
        params.socket_timeout = int(config.SOCKET_TIMEOUT)
        params.connection_attempts = int(config.CONNECTION_ATTEMPTS)
        params.retry_delay = int(config.RETRY_DELAY)

        self.conn = pika.BlockingConnection(params)
        self.channel = self.conn.channel()
        self._setup_infrastructure()

    def _setup_infrastructure(self) -> None:
        ch = self.channel
        ch.exchange_declare("radius_dlx", "direct", durable=True)
        ch.queue_declare("radius_dlq", durable=True, arguments={"x-queue-mode": "lazy"})
        ch.queue_bind("radius_dlq", "radius_dlx", "dlq")

        ch.exchange_declare("sessions_traffic_exchange", "direct", durable=True)
        ch.queue_declare(
            self.queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "radius_dlx",
                "x-dead-letter-routing-key": "dlq",
            },
        )
        ch.queue_bind(self.queue, "sessions_traffic_exchange", self.queue)
        ch.basic_qos(prefetch_count=int(config.PREFETCH_COUNT))

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        self.running = False
        if self.channel:
            self.channel.stop_consuming()
        if self.conn:
            self.conn.close()


# ----------------------------- #
#         SESSION CONSUMER      #
# ----------------------------- #
class SessionConsumer(RabbitConsumer):
    def __init__(self) -> None:
        super().__init__(queue="session_queue")
        self.ch_client = ClickHouseClient()

    def start(self) -> None:
        self.running = True
        try:
            self.connect()
            RABBITMQ_CONNECTIONS.labels(queue=self.queue, status="success").inc()
            SYSTEM_HEALTH.labels(component="rabbitmq").set(1)

            for method, _, body in self.channel.consume(
                self.queue, inactivity_timeout=1
            ):
                if not self.running:
                    break
                if body is None:
                    continue
                self._handle(body, method)

        except Exception as e:
            logging.error(f"SessionConsumer failed: {e}")
            RABBITMQ_CONNECTIONS.labels(queue=self.queue, status="failed").inc()
            SYSTEM_HEALTH.labels(component="rabbitmq").set(0)
            raise

    def _handle(self, body: bytes, method) -> None:
        with PROCESS_DURATION.labels(queue=self.queue, operation="total").time():
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                logging.error(f"Session JSON decode failed: {e}")
                self.channel.basic_nack(method.delivery_tag, requeue=False)
                TOTAL_MESSAGES.labels(queue=self.queue, status="json_error").inc()
                return

            # Валидация данных
            try:
                DataValidator.validate_session_data(data)
            except ValidationError as e:
                logging.warning(f"Session validation failed: {e}")
                self.channel.basic_nack(method.delivery_tag, requeue=False)
                TOTAL_MESSAGES.labels(queue=self.queue, status="validation_error").inc()
                return

            # Если всё ок — вставляем в ClickHouse
            try:
                self.ch_client.insert_session(data)
                self.channel.basic_ack(method.delivery_tag)
                TOTAL_MESSAGES.labels(queue=self.queue, status="success").inc()

            except Exception as e:
                logging.exception("Session processing failed")
                self.channel.basic_nack(method.delivery_tag, requeue=False)
                TOTAL_MESSAGES.labels(queue=self.queue, status="critical_error").inc()


# ----------------------------- #
#         TRAFFIC CONSUMER      #
# ----------------------------- #
class TrafficConsumer(RabbitConsumer):
    def __init__(self) -> None:
        super().__init__(queue="traffic_queue")
        self.ch_client = ClickHouseClient()
        self.batch: List[Dict[str, Any]] = []
        self.tags: List[int] = []
        self.last_flush = time.time()
        self.flush_interval = 5  # секунд

    def start(self) -> None:
        self.running = True
        try:
            self.connect()
            RABBITMQ_CONNECTIONS.labels(queue=self.queue, status="success").inc()
            SYSTEM_HEALTH.labels(component="rabbitmq").set(1)

            for method, _, body in self.channel.consume(
                self.queue, inactivity_timeout=1
            ):
                if not self.running:
                    break
                if body is None:
                    self._maybe_flush()
                    continue
                self._handle(body, method)

        except Exception as e:
            logging.error(f"TrafficConsumer failed: {e}")
            RABBITMQ_CONNECTIONS.labels(queue=self.queue, status="failed").inc()
            SYSTEM_HEALTH.labels(component="rabbitmq").set(0)
            raise

    def _handle(self, body: bytes, method) -> None:
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            logging.error(f"Traffic JSON decode failed: {e}")
            self.channel.basic_nack(method.delivery_tag, requeue=False)
            TOTAL_MESSAGES.labels(queue=self.queue, status="json_error").inc()
            return

        # Валидация данных
        try:
            DataValidator.validate_traffic_data(data)
        except ValidationError as e:
            logging.warning(f"Traffic validation failed: {e}")
            self.channel.basic_nack(method.delivery_tag, requeue=False)
            TOTAL_MESSAGES.labels(queue=self.queue, status="validation_error").inc()
            return

        # Если всё ок — добавляем в batch
        self.batch.append(data)
        self.tags.append(method.delivery_tag)
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        batch_size = len(self.batch)
        time_since_flush = time.time() - self.last_flush

        # Флашим если батч большой или прошло много времени
        should_flush = batch_size >= int(config.PREFETCH_COUNT) or (
            batch_size > 0 and time_since_flush > self.flush_interval
        )

        if should_flush:
            self._flush()

    def _flush(self) -> None:
        if not self.batch:
            return

        batch_size = len(self.batch)

        with PROCESS_DURATION.labels(queue=self.queue, operation="batch_flush").time():
            try:
                self.ch_client.insert_traffic_batch(self.batch)

                # Подтверждаем все сообщения в батче
                for tag in self.tags:
                    self.channel.basic_ack(tag)

                TOTAL_MESSAGES.labels(queue=self.queue, status="success").inc(
                    batch_size
                )
                logging.info(
                    f"Successfully processed batch of {batch_size} traffic records"
                )

            except Exception as e:
                logging.exception(f"Traffic batch processing failed: {e}")
                # Логируем проблемный батч для отладки
                logging.debug(f"Failed batch: {self.batch[:3]}...")  # Первые 3 элемента

                for tag in self.tags:
                    self.channel.basic_nack(tag, requeue=False)
                TOTAL_MESSAGES.labels(queue=self.queue, status="critical_error").inc(
                    batch_size
                )

            finally:
                self.batch.clear()
                self.tags.clear()
                self.last_flush = time.time()

    def stop(self) -> None:
        # Флашим оставшиеся данные перед остановкой
        if self.batch:
            logging.info(f"Flushing remaining {len(self.batch)} records on shutdown")
            self._flush()
        super().stop()


# ----------------------------- #
#         MAIN ENTRYPOINT       #
# ----------------------------- #


def update_system_metrics():
    """Обновление системных метрик"""
    try:
        # Память текущего процесса
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        MEMORY_USAGE.set(memory_info.rss)

        # Количество активных подключений
        connections = len(process.connections())
        ACTIVE_CONNECTIONS.labels(type="total").set(connections)

    except Exception as e:
        logging.warning(f"Failed to update system metrics: {e}")


def main() -> None:
    start_http_server(int(config.METRICS_PORT))

    consumers = [SessionConsumer(), TrafficConsumer()]
    threads = [threading.Thread(target=c.start, daemon=True) for c in consumers]

    def shutdown(sig, frame):
        for c in consumers:
            c.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Запуск потока для системных метрик
    def metrics_updater():
        while True:
            update_system_metrics()
            time.sleep(10)  # Обновляем каждые 10 секунд

    metrics_thread = threading.Thread(target=metrics_updater, daemon=True)
    metrics_thread.start()

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
