# rabbitmq_middleware.py
import pika
import orjson
import os
import time
from datetime import datetime
import uuid
from common.packet import FinalPacket
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_HEARTBEAT = int(os.getenv('RABBITMQ_HEARTBEAT', '1200'))

class Middleware:
    def __init__(self, queue, consumer_tag = None, exchange=None, exchange_type='direct', publish_to_exchange=True, routing_key=''):
        self.host = RABBITMQ_HOST
        self.consumer_tag = consumer_tag
        self.queue = queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.publish_to_exchange = publish_to_exchange 
        self.routing_key = routing_key
        self.connection = None
        self.channel = None
        self.is_consumed = False
        if not self.channel:
            self.connect()

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                port=RABBITMQ_PORT,
                heartbeat=RABBITMQ_HEARTBEAT))
        self.channel = self.connection.channel()
        if self.exchange:
            print(f"[Middleware] Declarando exchange '{self.exchange}' de tipo '{self.exchange_type}'...")
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=True)
            
            if self.queue:
                print(f"[Middleware] Declarando cola '{self.queue}' (durable=False)...")
                self.channel.queue_declare(queue=self.queue, durable=True)
                
                print(f"[Middleware] Enlazando cola '{self.queue}' al exchange '{self.exchange}'...")
                self.channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.routing_key)
        else:
            self.channel.queue_declare(queue=self.queue, durable=True)

    def publish(self, message, routing_key=''):
        if not self.channel:
            self.connect()
        if isinstance(message, bytes):  # Handle bytes from to_json()
            body = message
        elif isinstance(message, str):  # Handle str directly
            body = message.encode('utf-8')  # Convert to bytes for RabbitMQ
        else:  # Handle dict or other JSON-serializable objects
            body = orjson.dumps(message)  # Returns bytes
        if self.exchange and self.publish_to_exchange:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f" [x] Sent message to exchange {self.exchange} with routing key {routing_key}")

        else:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f" [x] Sent message to queue {self.queue}")

    
    def consume(self, callback):
        if not self.channel:
            self.connect()
        
        self.channel.basic_qos(prefetch_count=1)
        
        # Envolver el callback para actualizar is_consumed
        def wrapped_callback(ch, method, properties, body):
            self.is_consumed = True  # Activar is_consumed al recibir el primer mensaje
            callback(ch, method, properties, body)  # Llamar al callback original

        # Iniciar el consumo
        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=wrapped_callback,
            auto_ack=False,
            consumer_tag=self.consumer_tag
        )
        print(f" [*] Waiting for messages in {self.queue}")
        self.channel.start_consuming()
    
    # TODO: sacar client_id=0 como default
    def send_final(self, client_id=0, routing_key=''):
        """Publica un paquete FINAL a través de este middleware."""
        if not self.channel:
            self.connect()
        final_packet = FinalPacket(client_id)
        self.publish(final_packet.to_json(), routing_key)
        print(f"[Middleware] FinalPacket {final_packet.to_json()} enviado directamente.")
                
    def check_no_consumers(self):
        """Verifica si hay 0 consumidores en la cola de control."""
        if not self.channel:
            self.connect()
        result = self.channel.queue_declare(queue=self.queue, passive=True)
        consumer_count = result.method.consumer_count
        print(f" [x] Control queue has {consumer_count} active consumers")
        return consumer_count == 1
    
    def check_messages(self):
        if not self.channel:
            self.connect()
        result = self.channel.queue_declare(queue=self.queue, passive=True)
        count = result.method.message_count
        print(f" [x] Queue has {count} messages")
    
    def purge(self):
        if not self.channel:
            self.connect()
        self.channel.queue_purge(queue=self.queue)
        print(f"[Middleware] Cola '{self.queue}' purgada.")
        
    def close_graceful(self, method):
        if self.channel:
            self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        if self.connection and not self.connection.is_closed:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)

    def close(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            print(f"Failed to close connection. Error: {e}")
            
    def cancel_consumer(self):
        if self.channel and self.channel.is_open:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        if not self.is_consumed and self.channel and self.channel.is_open:
            self.connection.add_callback_threadsafe(lambda: self.channel.basic_cancel(self.consumer_tag))
            print("Consumidor cancelado exitosamente")

    def confirm_delivery(self):
        if self.channel:
            self.channel.confirm_delivery()    

    def delete_queue(self):
        if self.channel:
            self.channel.queue_delete(queue=self.queue)
            print(f"[Middleware] Cola '{self.queue}' eliminada.") 
