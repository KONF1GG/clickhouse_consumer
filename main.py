import logging
import signal
import ssl
import threading
import time
from typing import Any, Dict, List
import json
import pika
from clickhouse_driver import Client
from prometheus_client import Counter, Histogram, start_http_server
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
TOTAL_MESSAGES = Counter(
    "radius_messages_total",
    "Total processed messages",
    ["queue", "status"],  # status: success, transient, critical, unexpected
)

PROCESS_DURATION = Histogram(
    "radius_process_duration_seconds",
    "Time spent processing a message",
    ["queue"],
)


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

    def insert_session(self, data: Dict[str, Any]) -> None:
        """Insert single session into ClickHouse."""
        validate_session(data)
        row = prepare_session_row(data)
        self.client.execute(
            "INSERT INTO radius.radius_sessions_new (*) VALUES",
            [row],
        )

    def insert_traffic_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Insert traffic batch into ClickHouse."""
        rows = [prepare_traffic_row(item) for item in batch]
        self.client.execute(
            "INSERT INTO radius.radius_traffic (*) VALUES",
            rows,
        )


# ----------------------------- #
#         VALIDATION / PREP     #
# ----------------------------- #
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

NUMERIC_COLS = {
    "Acct-Session-Time",
    "Acct-Input-Octets", "Acct-Output-Octets",
    "Acct-Input-Packets", "Acct-Output-Packets",
    "Acct-Input-Gigawords", "Acct-Output-Gigawords",
    "ERX-IPv6-Acct-Input-Octets", "ERX-IPv6-Acct-Output-Octets",
    "ERX-IPv6-Acct-Input-Packets", "ERX-IPv6-Acct-Output-Packets",
    "ERX-IPv6-Acct-Input-Gigawords", "ERX-IPv6-Acct-Output-Gigawords",
    "GMT",
}

TRAFFIC_FIELDS = [
    "Acct-Unique-Session-Id",
    "login",
    "timestamp",
    "Acct-Input-Octets",
    "Acct-Output-Octets",
    "Acct-Input-Packets",
    "Acct-Output-Packets",
    "ERX-IPv6-Acct-Input-Octets",
    "ERX-IPv6-Acct-Output-Octets",
    "ERX-IPv6-Acct-Input-Packets",
    "ERX-IPv6-Acct-Output-Packets",
]


def validate_session(data: Dict[str, Any]) -> None:
    if not data:
        raise ValueError("Empty session data")

DATETIME_FIELDS = {"Acct-Start-Time", "Acct-Update-Time", "Acct-Stop-Time"}
DEFAULT_DT = datetime(1970, 1, 1, 5, 0, 0)

def parse_datetime_or_default(value: Any) -> datetime:
    """
    Преобразует строку/число в datetime.
    Если пусто — возвращает 1970-01-01 05:00:00.
    """
    if not value:
        return DEFAULT_DT
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return DEFAULT_DT


def prepare_session_row(data: Dict[str, Any]) -> List[Any]:
    clean = {k: v for k, v in data.items() if k in SESSION_FIELDS}

    # даты
    for dt_key in DATETIME_FIELDS:
        clean[dt_key] = parse_datetime_or_default(clean.get(dt_key))

    row = []
    for field in SESSION_FIELDS:
        value = clean.get(field)

        if field in DATETIME_FIELDS:
            row.append(value)
            continue

        if field in NUMERIC_COLS:
            # число: 0 по умолчанию, всегда int
            if field == 'GMT':
                value = 5
            try:
                row.append(int(float(value)))  # float -> int для "123.0"
            except (ValueError, TypeError):
                row.append(0)
        else:
            # строка: пусто -> ""
            val = str(value) if value is not None else ""
            row.append(val)

    return row

def prepare_traffic_row(data: Dict[str, Any]) -> List[Any]:
    return [
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
        self.connect()
        for method, _, body in self.channel.consume(self.queue, inactivity_timeout=1):
            if not self.running:
                break
            if body is None:
                continue
            self._handle(body, method)

    def _handle(self, body: bytes, method) -> None:
        with PROCESS_DURATION.labels(queue=self.queue).time():
            try:
                self.ch_client.insert_session(json.loads(body))
                self.channel.basic_ack(method.delivery_tag)
                TOTAL_MESSAGES.labels(queue=self.queue, status="success").inc()
            except Exception as e:
                logging.exception("Session insert failed")
                self.channel.basic_nack(method.delivery_tag, requeue=False)
                TOTAL_MESSAGES.labels(queue=self.queue, status="critical").inc()


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
        self.flush_interval = 5

    def start(self) -> None:
        self.running = True
        self.connect()
        for method, _, body in self.channel.consume(self.queue, inactivity_timeout=1):
            if not self.running:
                break
            if body is None:
                self._maybe_flush()
                continue
            self._handle(body, method)

    def _handle(self, body: bytes, method) -> None:
        self.batch.append(json.loads(body))
        self.tags.append(method.delivery_tag)
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        if time.time() - self.last_flush > self.flush_interval and self.batch:
            self._flush()

    def _flush(self) -> None:
        try:
            self.ch_client.insert_traffic_batch(self.batch)
            for tag in self.tags:
                self.channel.basic_ack(tag)
            TOTAL_MESSAGES.labels(queue=self.queue, status="success").inc(
                len(self.batch)
            )
        except Exception as e:
            logging.exception("Traffic batch insert failed")
            for tag in self.tags:
                self.channel.basic_nack(tag, requeue=False)
            TOTAL_MESSAGES.labels(queue=self.queue, status="critical").inc(
                len(self.batch)
            )
        finally:
            self.batch.clear()
            self.tags.clear()
            self.last_flush = time.time()

    def stop(self) -> None:
        self._flush()
        super().stop()


# ----------------------------- #
#         MAIN ENTRYPOINT       #
# ----------------------------- #
def main() -> None:
    start_http_server(int(config.METRICS_PORT))

    consumers = [SessionConsumer(), TrafficConsumer()]
    threads = [threading.Thread(target=c.start, daemon=True) for c in consumers]

    def shutdown(sig, frame):
        for c in consumers:
            c.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
