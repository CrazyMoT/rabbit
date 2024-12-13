import aio_pika
import asyncio
from loguru import logger
from rabbitmq_config import RABBITMQ_PARAMS

logger.add("consumer.log", rotation="1 MB")

MAX_RETRIES = 3  # Максимальное количество попыток

async def send_email(recipient, subject, body):
    # Замените эти строки на вашу реальную отправку электронной почты
    logger.info(f"Sending email to {recipient} with subject {subject} and body {body}")

async def handle_warning(recipient, subject, body):
    logger.warning(f"Warning for {recipient}: {subject} - {body}")

async def handle_info(recipient, subject, body):
    logger.info(f"Information for {recipient}: {subject} - {body}")

async def handle_error(recipient, subject, body):
    logger.error(f"Error for {recipient}: {subject} - {body}")

async def process_notification(notification_type, recipient, subject, body):
    if notification_type == 'email':
        await send_email(recipient, subject, body)
    elif notification_type == 'warning':
        await handle_warning(recipient, subject, body)
    elif notification_type == 'info':
        await handle_info(recipient, subject, body)
    elif notification_type == 'error':
        await handle_error(recipient, subject, body)
    else:
        logger.error(f"Unknown notification type: {notification_type}")

async def callback(message: aio_pika.IncomingMessage):
    try:
        async with message.process():
            notification_data = message.body.decode()

            notification_type, n_type, recipient, subject, body = notification_data.split(',')
            logger.info(f"Received {notification_type} notification for {recipient}")

            await process_notification(n_type, recipient, subject, body)
    except Exception as e:
        logger.error(f"Failed to process message: {e}")
        await handle_failed_message(message)

async def handle_failed_message(message: aio_pika.IncomingMessage):
    headers = message.headers
    retry_count = headers.get('x-retry-count', 0) + 1

    if retry_count > MAX_RETRIES:
        logger.error(f"Message {message.message_id} failed after {MAX_RETRIES} retries. Sending to DLX.")
        await message.reject(requeue=False)
    else:
        logger.warning(f"Retrying message {message.message_id}, attempt {retry_count}.")
        headers['x-retry-count'] = retry_count
        retry_message = aio_pika.Message(
            body=message.body,
            headers=headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await message.nack(requeue=False)
        await message.channel.default_exchange.publish(
            retry_message,
            routing_key=message.routing_key
        )

async def monitor_queue(channel: aio_pika.RobustChannel, queue_name: str):
    while True:
        try:
            queue = await channel.declare_queue(queue_name, passive=True)
            message_count = queue.declaration_result.message_count
            logger.info(f"Queue '{queue_name}' has {message_count} unprocessed messages.")
        except aio_pika.exceptions.AMQPError as e:
            logger.error(f"Failed to get queue status: {e}")
        await asyncio.sleep(10)  # Интервал мониторинга (например, каждые 10 секунд)

async def get_channel_and_queue(connection):
    channel = await connection.channel()

    # Объявление Dead Letter Exchange и Dead Letter Queue
    dlx = await channel.declare_exchange('dlx', aio_pika.ExchangeType.DIRECT, durable=True)
    dlx_queue = await channel.declare_queue('dlx_queue', durable=True)
    await dlx_queue.bind(dlx, routing_key='dlx_routing_key')

    # Попытка объявить очередь с проверкой на существование (passive=True)
    try:
        queue = await channel.declare_queue('email_queue', durable=True, passive=True)
        logger.info("Queue 'email_queue' already exists.")
    except aio_pika.exceptions.AMQPError:
        arguments = {
            'x-dead-letter-exchange': 'dlx',
            'x-dead-letter-routing-key': 'dlx_routing_key'
        }
        queue = await channel.declare_queue('email_queue', durable=True, arguments=arguments)
        logger.info("Queue 'email_queue' created with DLX parameters.")
    return channel, queue

async def start_consumer():
    while True:
        try:
            connection = await aio_pika.connect_robust(
                f'amqp://{RABBITMQ_PARAMS["user"]}:{RABBITMQ_PARAMS["password"]}@{RABBITMQ_PARAMS["host"]}:{RABBITMQ_PARAMS["port"]}/'
            )

            async with connection:
                channel, queue = await get_channel_and_queue(connection)

                # Получаем ссылку на уже существующий обменник типа direct
                exchange = await channel.declare_exchange('notification_exchange', aio_pika.ExchangeType.DIRECT, durable=True)

                # Привязываем очередь к обменнику через routing_key "email"
                await queue.bind(exchange, routing_key="email")

                # Запускаем мониторинг очереди
                asyncio.create_task(monitor_queue(channel, 'email_queue'))

                # Подключаемся к очереди для прослушивания сообщений
                await queue.consume(callback, no_ack=False)
                logger.info(' [*] Waiting for messages. To exit press CTRL+C')
                await asyncio.Future()  # Keep the program running
        except aio_pika.exceptions.AMQPError as e:
            logger.error(f"Connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Wait before reconnecting

async def send_notification(channel: aio_pika.RobustChannel, exchange_name: str, routing_key: str, body: str):
    try:
        exchange = await channel.declare_exchange(exchange_name, aio_pika.ExchangeType.DIRECT, durable=True)
        message = aio_pika.Message(body.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await exchange.publish(message, routing_key=routing_key)
        logger.info(f"Sent message to exchange '{exchange_name}' with routing key '{routing_key}'")
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_consumer())
