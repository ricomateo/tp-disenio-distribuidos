# filter.py
import json
from common.middleware import Middleware
from common.packet import DataPacket, MoviePacket, handle_final_packet, is_final_packet
import os

class RouterNode:
    """
    El RouterNode se suscribe a una 'input_queue', y envia los mensajes
    a un 'output_exchange', routeandolos segun un 'routing_key' que
    se determina segun: id % number_of_nodes, siendo:
     - id: el id de los mensajes,
     - number_of_nodes: la cantidad de nodos suscriptos al exchange 'output_exchange'.
    """
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.number_of_nodes = int(os.getenv("NUMBER_OF_NODES"))
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "")
        if self.input_queue is None:
            raise Exception("Missing RABBITMQ_QUEUE env var")
        if self.output_exchange is None:
            raise Exception("Missing RABBITMQ_OUTPUT_EXCHANGE env var")
        
        if self.exchange:
            # Si hay exchange lo usamos
            self.input_rabbitmq = Middleware(
                queue=self.input_queue,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False,
                routing_key=self.routing_key
            )
        else:
            # Sino conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)

        self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)


    def callback(self, ch, method, properties, body):
        """
        Recibe un mensaje y lo envia al output_exchange, routeandolo
        segun routing_key = msg.id % number_of_nodes.
        """
        try:
            packet_json = body.decode()
            
            if is_final_packet(json.loads(packet_json).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    for i in range(self.number_of_nodes):
                        self.output_rabbitmq.send_final(routing_key=str(i))
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            # Deserializo la peli para obtener el id
            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            movie_id = int(movie.get("id"))

            # Calculo la routing key como el modulo entre el id y la cantidad de nodos
            routing_key = str(movie_id % self.number_of_nodes)
            
            # Routeo el mensaje segun el routing key
            self.output_rabbitmq.publish(packet_json, routing_key=routing_key)
            print(f" [✓] Sent movie with id: {movie_id} through the exchange using routing key: {routing_key}")
            
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
