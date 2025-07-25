import logging
import ssl
import signal
import time
import json
import pika
from prometheus_client import Counter, Histogram, start_http_server
import threading
from clickhouse_driver import Client
import config


# ============================== #
#   Пользовательские исключения  #
# ============================== #
class TransientError(Exception):
    """Временная ошибка, можно повторить попытку"""

    pass


class CriticalError(Exception):
    """Критическая ошибка, требуется вмешательство"""

    pass


# ============================== #
#   Настройка метрик Prometheus  #
# ============================== #
PROCESSED_MSG = Counter("processed_messages", "Всего обработанных сообщений", ["queue"])
FAILED_MSG = Counter(
    "failed_messages", "Всего неудачных сообщений", ["queue", "reason"]
)
PROCESS_TIME = Histogram("process_time_seconds", "Время обработки сообщений", ["queue"])


# ==================== #
#   Клиент ClickHouse  #
# ==================== #
class ClickHouseClient:
    def __init__(self, host, port, user, password):
        self.client = Client(host=host, port=port, user=user, password=password)

    def insert_session(self, session_data):
        """Вставка данных сессии в ClickHouse"""

        if not session_data:
            raise ValueError("Session data is empty")

        # Поля которые отправляет freedom1.py в ch_save_session()
        session_fields = [
            "login",
            "onu_mac",
            "contract",
            "auth_type",
            "service",
            "Acct-Session-Id",
            "Acct-Unique-Session-Id",
            "Acct-Start-Time",
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

        # Обрабатываем временные поля - оставляем их как строки для ClickHouse
        time_fields = [
            "Acct-Start-Time",
            "Acct-Update-Time",
            "Acct-Stop-Time",
            "Event-Timestamp",
        ]
        for key in time_fields:
            if key in session_data and session_data[key] is not None:
                # Если это уже строка, проверяем формат и нормализуем
                if isinstance(session_data[key], str):
                    # Обрабатываем Event-Timestamp который приходит в формате "Jul 25 2025 13:26:16 +05"
                    if key == "Event-Timestamp" and " +" in session_data[key]:
                        try:
                            # Парсим и переформатируем в стандартный формат
                            import datetime

                            # Убираем часовой пояс для парсинга
                            time_part = session_data[key].split(" +")[0]
                            dt = datetime.datetime.strptime(
                                time_part, "%b %d %Y %H:%M:%S"
                            )
                            session_data[key] = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except Exception as e:
                            logging.warning(
                                f"Не удалось парсить Event-Timestamp '{session_data[key]}': {e}"
                            )
                            # Оставляем как есть, если не удалось парсить
                    continue
                # Если это объект datetime, конвертируем в строку
                elif hasattr(session_data[key], "strftime"):
                    session_data[key] = session_data[key].strftime("%Y-%m-%d %H:%M:%S")
                # Если это число (timestamp), конвертируем в строку
                elif isinstance(session_data[key], (int, float)):
                    import datetime

                    dt = datetime.datetime.fromtimestamp(session_data[key])
                    session_data[key] = dt.strftime("%Y-%m-%d %H:%M:%S")

        # Удаляем все поля, которые не входят в session_fields, чтобы избежать проблем с ClickHouse
        keys_to_remove = []
        for key in session_data.keys():
            if key not in session_fields:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del session_data[key]

        # Список полей, которые должны быть DateTime в ClickHouse
        datetime_fields = ["Acct-Start-Time", "Acct-Stop-Time"]

        # Список полей, которые должны быть числовыми в ClickHouse
        numeric_fields = [
            "Acct-Session-Time",
            "Acct-Input-Octets",
            "Acct-Output-Octets",
            "Acct-Input-Packets",
            "Acct-Output-Packets",
            "ERX-IPv6-Acct-Input-Octets",
            "ERX-IPv6-Acct-Output-Octets",
            "ERX-IPv6-Acct-Input-Packets",
            "ERX-IPv6-Acct-Output-Packets",
            "ERX-IPv6-Acct-Input-Gigawords",
            "ERX-IPv6-Acct-Output-Gigawords",
            "GMT",
        ]

        # Формируем список полей и значений, исключая пустые DateTime поля
        actual_fields = []
        values = []

        for field in session_fields:
            value = session_data.get(field)

            # Для DateTime полей: если значение пустое или None, просто пропускаем поле
            if field in datetime_fields and (value is None or value == ""):
                continue

            # Для поля GMT устанавливаем значение по умолчанию
            if value is None or value == "":
                if field == "GMT":
                    value = 5  # Числовое значение по умолчанию
                elif field in numeric_fields:
                    value = 0  # Для числовых полей 0 по умолчанию
                else:
                    value = ""  # Пустая строка для NULL значений
            else:
                # Обрабатываем числовые поля
                if field in numeric_fields:
                    try:
                        # Преобразуем в целое число
                        if isinstance(value, str):
                            value = int(
                                float(value)
                            )  # float потом int для обработки "123.0"
                        elif isinstance(value, float):
                            value = int(value)
                        elif not isinstance(value, int):
                            value = int(str(value))
                    except (ValueError, TypeError):
                        logging.warning(
                            f"Не удалось преобразовать {field}='{value}' в число, устанавливаем 0"
                        )
                        value = 0
                # Обрабатываем DateTime поля
                elif field in datetime_fields:
                    if isinstance(value, str) and value.strip():
                        try:
                            # Парсим строку в datetime объект
                            import datetime

                            # Пытаемся различные форматы
                            if ":" in value and "-" in value:  # YYYY-MM-DD HH:MM:SS
                                value = datetime.datetime.strptime(
                                    value, "%Y-%m-%d %H:%M:%S"
                                )
                            elif "/" in value:  # MM/DD/YYYY HH:MM:SS или другие форматы
                                # Добавьте другие форматы при необходимости
                                value = datetime.datetime.strptime(
                                    value, "%m/%d/%Y %H:%M:%S"
                                )
                            else:
                                # Если формат неизвестен, пробуем стандартный
                                value = datetime.datetime.strptime(
                                    value, "%Y-%m-%d %H:%M:%S"
                                )
                        except ValueError as e:
                            logging.warning(
                                f"Не удалось парсить DateTime поле {field}='{value}': {e}"
                            )
                            # Если не удалось парсить, пропускаем поле
                            continue
                    elif hasattr(value, "strftime"):
                        # Если это уже datetime объект, оставляем как есть
                        pass
                    elif isinstance(value, (int, float)):
                        import datetime

                        value = datetime.datetime.fromtimestamp(value)
                    else:
                        # Если значение не подходит для DateTime, пропускаем поле
                        continue
                # Для всех остальных полей преобразуем в строки
                else:
                    if not isinstance(value, str):
                        if isinstance(value, (int, float)):
                            value = str(value)
                        elif hasattr(value, "strftime"):
                            value = value.strftime("%Y-%m-%d %H:%M:%S")
                        elif value is None:
                            value = ""
                        else:
                            value = str(value)

                    # Убеждаемся что это строка для нечисловых полей
                    if (
                        field not in numeric_fields
                        and not isinstance(value, str)
                        and value is not None
                    ):
                        value = str(value) if value is not None else ""

            actual_fields.append(field)
            values.append(value)

        fields_str = ", ".join([f"`{field}`" for field in actual_fields])

        query = f"""
            INSERT INTO radius.radius_sessions_new 
            ({fields_str}) 
            VALUES
        """

        try:
            self.client.execute(query, [values])
        except Exception as e:
            # Логируем детальную информацию об ошибке
            import traceback

            logging.error("ClickHouse error details:")
            logging.error(f"Query: {query}")
            logging.error(f"Values: {values}")
            logging.error(f"Values types: {[type(v).__name__ for v in values]}")
            logging.error(f"Exception: {e}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    def insert_traffic_batch(self, traffic_data_list):
        """Вставка пачки данных трафика в ClickHouse"""
        if not traffic_data_list:
            raise ValueError("Traffic data is empty")

        # ВСЕ 11 полей которые отправляет freedom1.py в ch_save_traffic()
        required_fields = [
            "Acct-Unique-Session-Id",  # 1. ID сессии
            "login",  # 2. Логин
            "timestamp",  # 3. Время
            "Acct-Input-Octets",  # 4. IPv4 вход байты
            "Acct-Output-Octets",  # 5. IPv4 выход байты
            "Acct-Input-Packets",  # 6. IPv4 вход пакеты
            "Acct-Output-Packets",  # 7. IPv4 выход пакеты
            "ERX-IPv6-Acct-Input-Octets",  # 8. IPv6 вход байты
            "ERX-IPv6-Acct-Output-Octets",  # 9. IPv6 выход байты
            "ERX-IPv6-Acct-Input-Packets",  # 10. IPv6 вход пакеты
            "ERX-IPv6-Acct-Output-Packets",  # 11. IPv6 выход пакеты
        ]

        batch_data = []
        for data in traffic_data_list:
            missing_fields = []
            for field in required_fields:
                if field not in data and field != "timestamp":
                    missing_fields.append(field)

            if missing_fields:
                logging.warning(f"Отсутствуют поля: {missing_fields}")
                continue

            row = [
                data["Acct-Unique-Session-Id"],
                data["login"],
                int(float(data["timestamp"])),
                int(data["Acct-Input-Octets"]),
                int(data["Acct-Output-Octets"]),
                int(data["Acct-Input-Packets"]),
                int(data["Acct-Output-Packets"]),
                int(data["ERX-IPv6-Acct-Input-Octets"]),
                int(data["ERX-IPv6-Acct-Output-Octets"]),
                int(data["ERX-IPv6-Acct-Input-Packets"]),
                int(data["ERX-IPv6-Acct-Output-Packets"]),
            ]
            batch_data.append(row)

        if not batch_data:
            raise ValueError("No valid traffic data to insert")

        fields_str = ", ".join([f"`{field}`" for field in required_fields])
        query = f"""
            INSERT INTO radius.radius_traffic 
            ({fields_str}) 
            VALUES
        """

        self.client.execute(query, batch_data)


clickhouse_client = ClickHouseClient(
    host=config.CLICKHOUSE_HOST,
    port=config.CLICKHOUSE_PORT,
    user=config.CLICKHOUSE_USER,
    password=config.CLICKHOUSE_PASSWORD,
)


# ==================== #
#   Базовый класс потребителя  #
# ==================== #
class RabbitConsumer:
    def __init__(self, amqp_url, queue_name, clickhouse_client):
        self._connection = None
        self._channel = None
        self._url = amqp_url
        self._queue = queue_name
        self._reconnect_delay = 1
        self._max_reconnect_delay = 300
        self._running = False
        self.clickhouse_client = clickhouse_client

        # Настройка SSL
        self._ssl_enabled = False
        if config.CA_CERT_PATH:
            self._ssl_context = ssl.create_default_context(cafile=config.CA_CERT_PATH)
            self._ssl_context.load_cert_chain(
                certfile=config.CLIENT_CERT_PATH, keyfile=config.CLIENT_KEY_PATH
            )
            self._ssl_enabled = True

    def _connect(self):
        """Установка соединения с RabbitMQ"""
        logging.info("Подключение к RabbitMQ")
        params = pika.URLParameters(self._url)
        if self._ssl_enabled:
            params.ssl_options = pika.SSLOptions(self._ssl_context)
        params.socket_timeout = int(config.SOCKET_TIMEOUT)
        params.connection_attempts = int(config.CONNECTION_ATTEMPTS)
        params.retry_delay = int(config.RETRY_DELAY)
        return pika.BlockingConnection(params)

    def _reconnect(self):
        """Механизм переподключения"""
        while self._running:
            try:
                self._connection = self._connect()
                self._channel = self._connection.channel()
                self._setup_infrastructure()
                return
            except Exception as e:
                logging.error(
                    f"Ошибка переподключения: {e}. Повтор через {self._reconnect_delay}с"
                )
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

    def _setup_infrastructure(self):
        """Настройка инфраструктуры очередей и exchange"""
        # Настройка Dead Letter Exchange (DLX)
        self._channel.exchange_declare(
            exchange="radius_dlx", exchange_type="direct", durable=True
        )
        self._channel.queue_declare(
            queue="radius_dlq", durable=True, arguments={"x-queue-mode": "lazy"}
        )
        self._channel.queue_bind(
            queue="radius_dlq", exchange="radius_dlx", routing_key="dlq"
        )

        # Настройка основного exchange
        self._channel.exchange_declare(
            exchange="sessions_traffic_exchange", exchange_type="direct", durable=True
        )

        # Настройка основной очереди и привязка к exchange
        self._channel.queue_declare(
            queue=self._queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "radius_dlx",
                "x-dead-letter-routing-key": "dlq",
                "x-queue-type": "classic",
            },
        )
        self._channel.queue_bind(
            queue=self._queue,
            exchange="sessions_traffic_exchange",
            routing_key=self._queue,
        )
        self._channel.basic_qos(prefetch_count=int(config.PREFETCH_COUNT))

    def start(self):
        raise NotImplementedError("Дочерние классы должны реализовать этот метод")

    def stop(self):
        """Остановка потребителя"""
        self._running = False
        if self._channel:
            self._channel.stop_consuming()
        if self._connection:
            self._connection.close()

    def _on_message(self, channel, method, properties, body):
        raise NotImplementedError("Дочерние классы должны реализовать этот метод")

    def _send_to_dlq(self, body, error):
        """Отправка сообщения в DLQ"""
        pass  # Здесь можно добавить дополнительную логику, например, запись в лог


# ==================== #
#  Потребитель сессий  #
# ==================== #
class SessionConsumer(RabbitConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        """Запуск потребителя сессий"""
        self._running = True
        logging.info(f"Запуск потребителя сессий для {self._queue}")
        self._reconnect()
        try:
            while self._running:
                for message in self._channel.consume(self._queue, inactivity_timeout=1):
                    if not self._running:
                        break
                    method, properties, body = message
                    if body is not None:
                        self._on_message(self._channel, method, properties, body)
        except Exception as e:
            logging.error(f"Ошибка потребления: {e}")
            if self._running:
                time.sleep(10)
                self._reconnect()

    def _on_message(self, channel, method, properties, body):
        """Обработка сообщения о сессии"""
        start_time = time.time()
        try:
            if body is None:
                logging.info("Очередь пуста, сообщение не обработано.")
                return
            message = json.loads(body)
            self._process(message)
            channel.basic_ack(method.delivery_tag)
            PROCESSED_MSG.labels(queue=self._queue).inc()
            PROCESS_TIME.labels(queue=self._queue).observe(time.time() - start_time)
        except TransientError as e:
            logging.warning(f"Временная ошибка: {e}")
            channel.basic_nack(method.delivery_tag, requeue=True)
            FAILED_MSG.labels(queue=self._queue, reason="transient").inc()
        except CriticalError as e:
            logging.error(f"Критическая ошибка: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(queue=self._queue, reason="critical").inc()
            self._send_to_dlq(body, str(e))
        except Exception as e:
            logging.exception("Непредвиденная ошибка")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(queue=self._queue, reason="unexpected").inc()
            self._send_to_dlq(body, str(e))

    def _process(self, message):
        """Обработка сообщения о сессии"""
        try:
            # Добавляем логирование для отладки
            logging.debug(f"Обрабатываем сессию: {message}")
            self.clickhouse_client.insert_session(message)
        except Exception as e:
            # Логируем детали ошибки
            logging.error(f"Ошибка при записи сессии. Данные: {message}. Ошибка: {e}")
            raise TransientError(f"Не удалось записать сессию: {e}")


# ==================== #
# Потребитель трафика  #
# ==================== #
class TrafficConsumer(RabbitConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._traffic_batch = []  # Пакет сообщений
        self.clickhouse_client = clickhouse_client
        self._last_batch_time = time.time()  # Время последней отправки пачки
        self._batch_interval = 5  # Интервал отправки пачки в секундах
        self._running = False

    def start(self):
        """Запуск потребителя трафика"""
        self._running = True
        logging.info(f"Запуск потребителя трафика для {self._queue}")
        self._reconnect()
        try:
            while self._running:
                for message in self._channel.consume(self._queue, inactivity_timeout=1):
                    if not self._running:
                        break
                    method, properties, body = message
                    if body is None:
                        self._check_and_send_batch()
                    else:
                        self._on_message(self._channel, method, properties, body)

                self._check_and_send_batch()

        except Exception as e:
            logging.error(f"Ошибка потребления: {e}")
            if self._running:
                time.sleep(10)
                self._reconnect()

    def _on_message(self, channel, method, properties, body):
        """Обработка сообщения о трафике"""
        try:
            if body is None:
                logging.info("Очередь пуста, сообщение не обработано.")
                return
            message = json.loads(body)
            self._traffic_batch.append((message, method.delivery_tag))
        except Exception as e:
            logging.error(f"Ошибка обработки сообщения: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)
            self._send_to_dlq(body, str(e))

    def _check_and_send_batch(self):
        """Проверка и отправка пачки сообщений, если прошло достаточно времени"""
        if (
            time.time() - self._last_batch_time > self._batch_interval
            and self._traffic_batch
        ):
            self._insert_batch()
            self._last_batch_time = (
                time.time()
            )  # Обновляем время последней отправки пачки

    def _insert_batch(self):
        """Вставка пачки сообщений о трафике в Clickhouse"""
        if not self._traffic_batch:
            return

        messages = [msg for msg, tag in self._traffic_batch]
        start_time = time.time()

        try:
            clickhouse_client.insert_traffic_batch(messages)
            for _, tag in self._traffic_batch:
                self._channel.basic_ack(tag)  # Подтверждаем обработку сообщений
            PROCESSED_MSG.labels(queue=self._queue).inc(len(self._traffic_batch))
            PROCESS_TIME.labels(queue=self._queue).observe(time.time() - start_time)
        except Exception as e:
            logging.error(f"Не удалось записать пачку: {e}")
            for _, tag in self._traffic_batch:
                self._channel.basic_nack(tag, requeue=False)
                self._send_to_dlq(json.dumps(messages), str(e))
            FAILED_MSG.labels(queue=self._queue, reason="batch_failure").inc(
                len(self._traffic_batch)
            )
        finally:
            self._traffic_batch.clear()  # Очищаем пакет сообщений после отправки

    def stop(self):
        """Остановка потребителя с сохранением последней пачки"""
        if self._traffic_batch:
            self._insert_batch()
        super().stop()


# ==================== #
#   Основной запуск    #
# ==================== #
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,  # Изменено с WARNING на INFO для отладки
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(config.LOG_PATH), logging.StreamHandler()],
    )

    start_http_server(int(config.METRICS_PORT))

    # Создаем экземпляры клиента ClickHouse для каждого потока
    clickhouse_client_session = ClickHouseClient(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
    )

    clickhouse_client_traffic = ClickHouseClient(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
    )

    # Создаем экземпляры для каждого потребителя с разными клиентами
    session_consumer = SessionConsumer(
        amqp_url=config.AMQP_URL,
        queue_name="session_queue",
        clickhouse_client=clickhouse_client_session,
    )

    traffic_consumer = TrafficConsumer(
        amqp_url=config.AMQP_URL,
        queue_name="traffic_queue",
        clickhouse_client=clickhouse_client_traffic,
    )

    def signal_handler(sig, frame):
        """Обработчик сигналов для graceful shutdown"""
        session_consumer.stop()
        traffic_consumer.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    session_thread = threading.Thread(target=session_consumer.start)
    traffic_thread = threading.Thread(target=traffic_consumer.start)

    session_thread.start()
    traffic_thread.start()

    session_thread.join()
    traffic_thread.join()
