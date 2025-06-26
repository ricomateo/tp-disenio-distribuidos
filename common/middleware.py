# rabbitmq_middleware.py
import pika
import orjson
import os
import logging
from common.packet import DeletePacketWithNodeId, FinalPacket, FinalPacketWithNodeId, DeletePacket
from common.logger import init_logging

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_HEARTBEAT = int(os.getenv('RABBITMQ_HEARTBEAT', '1200'))

init_logging(os.getenv("LOG_LEVEL", "info"))

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
            logging.info("[Middleware] Declarando exchange '%s' de tipo '%s'...", self.exchange, self.exchange_type)
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=True)
            
            if self.queue:
                logging.info("[Middleware] Declarando cola '%s' (durable=True)...", self.queue)
                self.channel.queue_declare(queue=self.queue, durable=True)
                logging.info("[Middleware] Enlazando cola '%s' al exchange '%s'...", self.queue, self.exchange)
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
            logging.debug("Sent message to exchange %s with routing key %s", self.exchange, routing_key)

        else:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue,
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logging.debug("Sent message to queue %s", self.queue)

    
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
        logging.info("Waiting for messages in %s", self.queue)
        self.channel.start_consuming()
        
    # TODO: sacar client_id=0 como default
    def send_delete(self, client_id=0, routing_key=''):
        """Publica un paquete DELETE a través de este middleware."""
        if not self.channel:
            self.connect()
        final_packet = DeletePacket(client_id)
        self.publish(final_packet.to_json(), routing_key)
        logging.debug("[Middleware] DeletePacket %s enviado directamente.", final_packet.to_json())

    def send_delete_with_node_id(self, client_id, node_id, routing_key=''):
        """Publica un paquete DELETE (con node_id) a través de este middleware."""
        if not self.channel:
            self.connect()
        delete_packet = DeletePacketWithNodeId(client_id=client_id, node_id=node_id)
        self.publish(delete_packet.to_json(), routing_key)
        logging.debug("[Middleware] DeletePacketWithNodeId %s enviado directamente.", delete_packet.to_json())

    # TODO: sacar client_id=0 como default
    def send_final(self, client_id=0, routing_key='', count=0):
        """Publica un paquete FINAL a través de este middleware."""
        if not self.channel:
            self.connect()
        final_packet = FinalPacket(client_id, count)
        self.publish(final_packet.to_json(), routing_key)
        logging.debug("[Middleware] FinalPacket %s enviado directamente.", final_packet.to_json())

    def send_final_with_node_id(self, client_id, node_id, count, routing_key=''):
        """Publica un paquete FINAL (con node_id) a través de este middleware."""
        if not self.channel:
            self.connect()
        final_packet = FinalPacketWithNodeId(client_id=client_id, node_id=node_id, count=count)
        self.publish(final_packet.to_json(), routing_key)
        logging.debug("[Middleware] FinalPacketWithNodeId %s enviado directamente.",final_packet.to_json())
        
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
            logging.error("Failed to close connection. Error: %s", e)
            
    def cancel_consumer(self):
        if self.channel and self.channel.is_open:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        if not self.is_consumed and self.channel and self.channel.is_open:
            self.connection.add_callback_threadsafe(lambda: self.channel.basic_cancel(self.consumer_tag))
            logging.info("Consumidor cancelado exitosamente")

    def confirm_delivery(self):
        if self.channel:
            self.channel.confirm_delivery()    

    def delete_queue(self):
        if self.channel:
            self.channel.queue_delete(queue=self.queue)
            logging.info("[Middleware] Cola '%s' eliminada.", self.queue) 
