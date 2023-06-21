from os import getenv
from time import sleep

from django.http import HttpResponse
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer
from django.core.handlers.wsgi import WSGIRequest


load_dotenv()


def kafka_producer(request: WSGIRequest) -> HttpResponse:
    bootstrap_servers = getenv('KAFKA_BOOTSTRAP_SERVERS')
    print(f'{bootstrap_servers = }')
    topic = getenv('KAFKA_TOPIC')
    print(f"{topic = }")
    # message = "Test message"
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    for i_message_num in range(100):
        message = f'Test message {i_message_num}'

        producer.produce(topic, message.encode('utf-8'))
        producer.flush()

        sleep(0.5)

    return HttpResponse('PRODUCER')


def kafka_consumer(request: WSGIRequest) -> HttpResponse:
    bootstrap_servers = getenv('KAFKA_BOOTSTRAP_SERVERS')
    topic = getenv('KAFKA_TOPIC')

    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    while True:
        message = consumer.poll(timeout=1.0)
        print(f'{message = }')
        if message is None:
            break

        if message.error():
            print(f'Consumer error: {message.error()}')
            continue

        print(f'Received message: {message.value().decode("utf-8")}')

    consumer.close()

    return HttpResponse('CONSUMER')
