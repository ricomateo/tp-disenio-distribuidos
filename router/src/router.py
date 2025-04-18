# filter.py
import json
import math
from common.middleware import Middleware
from common.packet import MoviePacket, handle_final_packet, is_final_packet
from datetime import datetime
import os
class RouterNode:
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.number_of_nodes = int(os.getenv("NUMBER_OF_NODES"))
        if self.input_queue is None:
            raise Exception("Missing RABBITMQ_QUEUE env var")
        if self.exchange is None:
            raise Exception("Missing RABBITMQ_EXCHANGE env var")
        if self.output_exchange is None:
            raise Exception("Missing RABBITMQ_OUTPUT_EXCHANGE env var")
        self.input_rabbitmq = Middleware(queue=self.input_queue, exchange=self.exchange, consumer_tag=self.consumer_tag, publish_to_exchange=False)
        self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange, exchange_type='direct')


    def callback(self, ch, method, properties, body):
        try:
            packet_json = body.decode()
            
            if is_final_packet(json.loads(packet_json).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            # Deserializo la peli para obtener el id
            packet = MoviePacket.from_json(packet_json)
            movie = packet.movie
            movie_id = int(movie.get("id"))

            # Calculo la routing key como el modulo entre el id y la cantidad de nodos
            routing_key = str(movie_id % self.number_of_nodes)
            
            # Routeo el mensaje segun el routing key
            self.output_rabbitmq.publish(packet_json, routing_key=routing_key)
            print(f" [✓] Sent movie with id: {movie_id} through the exchange: {routing_key}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)


    def start_node(self):
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in router node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()
