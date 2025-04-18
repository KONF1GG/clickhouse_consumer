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
from datetime import datetime

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
PROCESSED_MSG = Counter('processed_messages', 'Всего обработанных сообщений', ['queue'])
FAILED_MSG = Counter('failed_messages', 'Всего неудачных сообщений', ['queue', 'reason'])
PROCESS_TIME = Histogram('process_time_seconds', 'Время обработки сообщений', ['queue'])

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

        fields = []
        values = []

        time_fields = ["Acct-Start-Time", "Acct-Update-Time", "Acct-Stop-Time"]

        for key, value in session_data.items():
            if key in time_fields and isinstance(value, str):
                session_data[key] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                value = session_data[key] 

            fields.append(f'"{key}"')
            values.append(f"'{value}'" if isinstance(value, str) else value)

        query = f"""
            INSERT INTO radius.radius_sessions (
                {', '.join(fields)}
            ) VALUES
        """
        values_list = [session_data]
        self.client.execute(query, values_list)
    
    def insert_traffic_batch(self, traffic_data_list):
        """Вставка пачки данных трафика в ClickHouse"""
        if not traffic_data_list:
            raise ValueError("Traffic data is empty")

        all_fields = [
            "`Acct-Unique-Session-Id`",
            "`login`",
            "`Acct-Input-Octets`",
            "`Acct-Output-Octets`",
            "`Acct-Input-Packets`",
            "`Acct-Output-Packets`",
            "`ERX-IPv6-Acct-Input-Octets`",
            "`ERX-IPv6-Acct-Output-Octets`",
            "`ERX-IPv6-Acct-Input-Packets`",
            "`ERX-IPv6-Acct-Output-Packets`",
            "`timestamp`"
        ]

        values = []
        for data in traffic_data_list:
            value_list = []
            for field in all_fields:
                field_name = field.strip("`")
                if field_name in data:
                    value = data[field_name]
                else:
                    if field_name == "timestamp":
                        value = "now()"
                    elif isinstance(data.get(field_name), str):
                        value = "''"
                    else:
                        value = "0" 

                value_list.append(f"'{value}'" if isinstance(value, str) else str(value))

            values.append(f"({', '.join(value_list)})")

        query = f"""
            INSERT INTO radius.radius_traffic (
                {', '.join(all_fields)}
            ) VALUES
                {', '.join(values)}
        """

        self.client.execute(query)





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
            self._ssl_context.load_cert_chain(certfile=config.CLIENT_CERT_PATH, keyfile=config.CLIENT_KEY_PATH)
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
                logging.error(f"Ошибка переподключения: {e}. Повтор через {self._reconnect_delay}с")
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    def _setup_infrastructure(self):
        """Настройка инфраструктуры очередей и exchange"""
        # Настройка Dead Letter Exchange (DLX)
        self._channel.exchange_declare(exchange='radius_dlx', exchange_type='direct', durable=True)
        self._channel.queue_declare(queue='radius_dlq', durable=True, arguments={'x-queue-mode': 'lazy'})
        self._channel.queue_bind(queue='radius_dlq', exchange='radius_dlx', routing_key='dlq')

        # Настройка основного exchange
        self._channel.exchange_declare(exchange='sessions_traffic_exchange', exchange_type='direct', durable=True)

        # Настройка основной очереди и привязка к exchange
        self._channel.queue_declare(
            queue=self._queue,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'radius_dlx',
                'x-dead-letter-routing-key': 'dlq',
                'x-queue-type': 'classic'
            }
        )
        self._channel.queue_bind(
            queue=self._queue,
            exchange='sessions_traffic_exchange',
            routing_key=self._queue
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
            FAILED_MSG.labels(queue=self._queue, reason='transient').inc()
        except CriticalError as e:
            logging.error(f"Критическая ошибка: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(queue=self._queue, reason='critical').inc()
            self._send_to_dlq(body, str(e))
        except Exception as e:
            logging.exception("Непредвиденная ошибка")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(queue=self._queue, reason='unexpected').inc()
            self._send_to_dlq(body, str(e))

    def _process(self, message):
        """Обработка сообщения о сессии"""
        try:
            clickhouse_client.insert_session(message)
        except Exception as e:
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
        if time.time() - self._last_batch_time > self._batch_interval and self._traffic_batch:
            self._insert_batch()
            self._last_batch_time = time.time()  # Обновляем время последней отправки пачки

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
            FAILED_MSG.labels(queue=self._queue, reason='batch_failure').inc(len(self._traffic_batch))
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
        level=logging.WARNING,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(config.LOG_PATH),
            logging.StreamHandler()
        ]
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
        queue_name='session_queue',
        clickhouse_client=clickhouse_client_session
    )

    traffic_consumer = TrafficConsumer(
        amqp_url=config.AMQP_URL,
        queue_name='traffic_queue',
        clickhouse_client=clickhouse_client_traffic
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