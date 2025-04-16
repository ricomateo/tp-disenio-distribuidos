# rabbitmq_middleware.py
import pika
import json
import os
import time
from datetime import datetime
import uuid
from common.packet import FinalPacket
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_HEARTBEAT = int(os.getenv('RABBITMQ_HEARTBEAT', '1200'))

class Middleware:
    def __init__(self, queue, consumer_tag = None, exchange=None, exchange_type='fanout', publish_to_exchange=True):
        self.host = RABBITMQ_HOST
        self.consumer_tag = consumer_tag
        self.queue = queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.publish_to_exchange = publish_to_exchange 
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                port=RABBITMQ_PORT,
                heartbeat=RABBITMQ_HEARTBEAT))
        self.channel = self.connection.channel()
        if self.exchange:
            print(f"[Middleware] Declarando exchange '{self.exchange}' de tipo '{self.exchange_type}'...")
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
            
            if self.queue:
                print(f"[Middleware] Declarando cola '{self.queue}' (durable=True)...")
                self.channel.queue_declare(queue=self.queue, durable=False)
                
                print(f"[Middleware] Enlazando cola '{self.queue}' al exchange '{self.exchange}'...")
                self.channel.queue_bind(queue=self.queue, exchange=self.exchange)
        else:
            self.channel.queue_declare(queue=self.queue, durable=False)

    def publish(self, message):
        if not self.channel:
            self.connect()
        body = message if isinstance(message, str) else json.dumps(message)
        if self.exchange and self.publish_to_exchange:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key='',  # Fanout ignores routing_key
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            #print(f" [x] Sent message to exchange {self.exchange}")
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
        self.channel.basic_consume(queue=self.queue, on_message_callback=callback, auto_ack=False, consumer_tag=self.consumer_tag)
        print(f" [*] Waiting for messages in {self.queue}")
        self.channel.start_consuming()
        
    def send_final(self):
        """Publica un paquete FINAL a través de este middleware."""
        if not self.channel:
            self.connect()
        final_packet = FinalPacket(timestamp=datetime.utcnow().isoformat())
        self.publish(final_packet.to_json())
        print(f"[Middleware] FinalPacket enviado directamente.")
    
    def send_final_until_no_consumers(self, method):
            """Envía FINAL PACKET hasta que no haya consumidores y purga la cola al final."""
            if not self.check_no_consumers():
                print(" [x] Sending FINAL PACKET...")
                
                final_packet = FinalPacket(timestamp=datetime.utcnow().isoformat(),)
                self.publish(final_packet.to_json())
                self.send_ack_and_close(method)
                return False
            return True
    
    def send_ack_and_close(self, method):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        self.channel.stop_consuming()
        self.close()
                
    def check_no_consumers(self):
        """Verifica si hay 0 consumidores en la cola de control."""
        if not self.channel:
            self.connect()
        result = self.channel.queue_declare(queue=self.queue, passive=True)
        consumer_count = result.method.consumer_count
        print(f" [x] Control queue has {consumer_count} active consumers")
        return consumer_count == 1
    
    def purge(self):
        if not self.channel:
            self.connect()
        self.channel.queue_purge(queue=self.queue)
        print(f"[Middleware] Cola '{self.queue}' purgada.")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()