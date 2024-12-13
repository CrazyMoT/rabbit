import json
import random
import time
import requests

# URL FastAPI-приложения
API_URL = "http://localhost:5000/send_notification"

# Возможные типы уведомлений
NOTIFICATION_TYPES = ["email", "sms", "warning", "info", "error"]

def send_to_producer():
    notification_type = random.choice(NOTIFICATION_TYPES)  # Выбор случайного типа уведомления
    message_json = {
        "notification_type": "email",
        "type": notification_type,
        "recipient": "user@lol.com",
        "subject": f"Notification: Hello {notification_type}!",
        "body": f"This is a test {notification_type}"
    }
    # Отправка данных в FastAPI-приложение
    headers = {'Content-Type': 'application/json'}
    response = requests.post(API_URL, json=message_json, headers=headers)

    if response.status_code == 200:
        print(f"Message sent successfully: {response.json()}")
    else:
        print(f"Failed to send message: {response.status_code}, {response.text}")

if __name__ == '__main__':
    for _ in range(10):  # Генерация 10 сообщений
        send_to_producer()
        time.sleep(1)
