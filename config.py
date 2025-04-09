from dotenv import load_dotenv
import os
load_dotenv()

# ==================== #
#   RabbitMQ Settings  #
# ==================== #
AMQP_URL = os.getenv('AMQP_URL')
QUEUE_NAME = os.getenv('QUEUE_NAME')

# ==================== #
#   SSL Configuration  #
# ==================== #
CA_CERT_PATH = os.getenv('CA_CERT_PATH')
CLIENT_CERT_PATH = os.getenv('CLIENT_CERT_PATH')
CLIENT_KEY_PATH = os.getenv('CLIENT_KEY_PATH')

# ==================== #
#   Network Settings   #
# ==================== #
SOCKET_TIMEOUT = os.getenv('SOCKET_TIMEOUT', '10')
CONNECTION_ATTEMPTS = os.getenv('CONNECTION_ATTEMPTS', '3')
RETRY_DELAY = os.getenv('RETRY_DELAY', '5')
PREFETCH_COUNT = os.getenv('PREFETCH_COUNT', '100')

# ==================== #
#   Monitoring         #
# ==================== #
METRICS_PORT = os.getenv('METRICS_PORT', '8000')
LOG_PATH = os.path.join(os.path.dirname(__file__), 'logs', 'consumer.log')

# ==================== #
#  CLickhouse Settings #
# ==================== #
CLICKHOUSE_HOST= os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT= os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_USER= os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD= os.getenv('CLICKHOUSE_PASSWORD')
