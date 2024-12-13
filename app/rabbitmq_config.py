import os

# Чтение переменных окружения или использование значений по умолчанию
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'password')

# Настройка RabbitMQ
RABBITMQ_PARAMS = {
    'host': RABBITMQ_HOST,
    'port': RABBITMQ_PORT,
    'user': RABBITMQ_USER,
    'password': RABBITMQ_PASS,
}


