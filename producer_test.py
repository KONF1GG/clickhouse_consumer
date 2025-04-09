import pika
import json
import time
from config import AMQP_URL

# Пример сообщения для сессии
session_message = {
    "login": "user1",
    "onu_mac": "00:1A:2B:3C:4D:5E",
    "contract": "contract123",
    "Acct-Session-Id": "session_001",
    "Acct-Unique-Session-Id": "unique_001",
    "Acct-Start-Time": "2025-04-07 10:00:00",
    "Acct-Input-Octets": 1024,
    "Acct-Output-Octets": 2048
}

# Пример сообщения для трафика
traffic_message = {
    "Acct-Unique-Session-Id": "unique_002",
    "login": "user1",
    "Acct-Input-Octets": 512,
    "Acct-Output-Octets": 1024,
    "timestamp": "2025-04-07 10:01:00"
}

class RabbitMQClient:
    def __init__(self, amqp_url):
        """Инициализация клиента RabbitMQ"""
        self.connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        self.channel = self.connection.channel()
    
    def declare_exchange(self, exchange_name):
        """Объявление exchange"""
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

    def send_message(self, exchange_name, routing_key, message):
        """Отправка сообщения через exchange в RabbitMQ"""
        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=json.dumps(message).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"Отправлено сообщение в exchange '{exchange_name}' с routing_key '{routing_key}': {message}")

    def close(self):
        """Закрытие соединения"""
        self.connection.close()

def main():
    """Отправка тестовых сообщений через exchange"""
    exchange_name = "sessions_traffic_exchange"

    # Создание клиента RabbitMQ
    client = RabbitMQClient(AMQP_URL)
    client.declare_exchange(exchange_name)
    client.send_message(exchange_name, "session_queue", session_message)
    # client.send_message(exchange_name, "traffic_queue", traffic_message)

    client.close()

if __name__ == "__main__":
    main()
