from django.urls import path

from .views import kafka_producer, kafka_consumer


app_name = 'kakafka'

urlpatterns = [
    path('producer/', kafka_producer, name='producer'),
    path('consumer/', kafka_consumer, name='consumer'),
]
