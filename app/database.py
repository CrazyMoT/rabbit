import aio_pika
import asyncio
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from rabbitmq_config import RABBITMQ_PARAMS

# Конфигурация базы данных
DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Модель базы данных
class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    notification_type = Column(String, index=True)
    recipient = Column(String, index=True)
    subject = Column(String, index=True, nullable=True)
    body = Column(Text)

# Создание таблиц
Base.metadata.create_all(bind=engine)

async def save_to_db(notification_type, recipient, subject, body):
    db = SessionLocal()
    notification = Notification(
        notification_type=notification_type,
        recipient=recipient,
        subject=subject,
        body=body
    )
    db.add(notification)
    db.commit()
    db.refresh(notification)
    db.close()
    print(f"Saved {notification_type} notification for {recipient} to database")

async def callback(message: aio_pika.IncomingMessage):
    async with message.process():
        notification_data = message.body.decode()
        notification_type, recipient, subject, body = notification_data.split(',')
        print(f"Received {notification_type} notification for {recipient}")
        await save_to_db(notification_type, recipient, subject, body)

async def start_consumer():
    connection = await aio_pika.connect_robust(
        f'amqp://{RABBITMQ_PARAMS["user"]}:{RABBITMQ_PARAMS["password"]}@{RABBITMQ_PARAMS["host"]}:{RABBITMQ_PARAMS["port"]}/'
    )
    async with connection:
        channel = await connection.channel()

        # Получаем ссылку на уже существующий обменник типа direct
        exchange = await channel.declare_exchange('notification_exchange', aio_pika.ExchangeType.DIRECT, durable=True)

        # Получаем ссылку на уже существующую очередь
        queue = await channel.declare_queue('notification_queue', durable=True)

        # Привязываем очередь к обменнику через routing_key "email"
        await queue.bind(exchange, routing_key="email")

        # Подключаемся к очереди для прослушивания сообщений
        await queue.consume(callback, no_ack=False)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        await asyncio.Future()  # Keep the program running

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_consumer())
