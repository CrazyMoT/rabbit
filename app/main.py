from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
from rabbitmq_config import RABBITMQ_PARAMS
import aio_pika
from loguru import logger

app = FastAPI()
logger.add("app.log", rotation="1 MB")


class RabbitMQ:
    MAX_RETRIES = 3  # Максимальное количество попыток
    # Асинхронное подключение к RabbitMQ
    async def get_rabbitmq_channel(self):
        # Формируем строку подключения
        connection_url = f"amqp://{RABBITMQ_PARAMS['user']}:{RABBITMQ_PARAMS['password']}@{RABBITMQ_PARAMS['host']}:{RABBITMQ_PARAMS['port']}/"

        # Подключаемся к RabbitMQ
        connection = await aio_pika.connect_robust(connection_url)
        channel = await connection.channel()  # Асинхронное создание канала

        return channel

    # Инициализация RabbitMQ
    async def init_rabbitmq(self):
        channel = await self.get_rabbitmq_channel()

        # Объявляем обменник
        await channel.declare_exchange('notification_exchange', type='direct', durable=True)

        # Объявляем очереди
        email_queue = await channel.declare_queue('email_queue', durable=True)
        sms_queue = await channel.declare_queue('sms_queue', durable=True)

        # Привязываем очереди к обменнику с соответствующими routing_key
        await email_queue.bind('notification_exchange', routing_key='email')
        await sms_queue.bind('notification_exchange', routing_key='sms')

        return channel

    # Публикация сообщения в RabbitMQ
    async def publish_message(self, notification_type, n_type, recipient, subject, body):
        channel = await self.init_rabbitmq()
        exchange = await channel.get_exchange('notification_exchange')
        message = f"{notification_type},{n_type},{recipient},{subject},{body}"
        await exchange.publish(
            aio_pika.Message(body=message.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=notification_type
        )
        logger.info(f" [x] Sent '{message}'")


    # Обработка сообщений из очереди
    async def handle_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                notification_data = message.body.decode()
                notification_type, recipient, subject, body = notification_data.split(',')
                logger.info(f"Received {notification_type} notification for {recipient}")
                await self.process_notification(notification_type, recipient, subject, body)
                await message.ack()  # Подтверждение успешной обработки сообщения
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                await self.handle_failed_message(message)

    async def process_notification(self, notification_type, recipient, subject, body):
        if notification_type == 'email':
            await self.send_email(recipient, subject, body)
        elif notification_type == 'sms':
            await self.send_sms(recipient, subject, body)
        else:
            logger.error(f"Unknown notification type: {notification_type}")

    async def send_email(self, recipient, subject, body):
        logger.info(f"Sending email to {recipient} with subject {subject} and body {body}")

    async def send_sms(self, recipient, subject, body):
        logger.info(f"Sending SMS to {recipient} with subject {subject} and body {body}")

    async def handle_failed_message(self, message: aio_pika.IncomingMessage):
        headers = message.headers
        retry_count = headers.get('x-retry-count', 0) + 1

        if retry_count > RabbitMQ.MAX_RETRIES:
            logger.error(f"Message {message.message_id} failed after {RabbitMQ.MAX_RETRIES} retries. Sending to DLX.")
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

# Модель запроса
class NotificationRequest(BaseModel):
    notification_type: str = "email"
    type: str
    recipient: str
    subject: str
    body: str

# Маршрут для отправки уведомлений
@app.post("/send_notification")
async def send_notification(request: NotificationRequest):
    try:
        await RabbitMQ().publish_message(request.notification_type, request.type, request.recipient, request.subject, request.body)
        return {"status": "Message sent to queue"}
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await RabbitMQ().init_rabbitmq()
    yield
    logger.info("Shutting down application")

# Запуск приложения
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
