import json
from common.middleware import Middleware
from common.packet import is_final_packet
import os
import signal

from common.worker_protocol import WorkerProtocol

class RouterNode:
    """
    El RouterNode se suscribe a una 'input_queue', y envia los mensajes
    a un 'output_exchange', routeandolos segun un 'routing_key' que
    se determina segun: id % number_of_nodes, siendo:
     - id: el id de los mensajes,
     - number_of_nodes: la cantidad de nodos suscriptos al exchange 'output_exchange'.
    """
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.input_queue = os.getenv("RABBITMQ_QUEUE")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.number_of_nodes = int(os.getenv("NUMBER_OF_NODES"))
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "")
        self.router_by = os.getenv("ROUTER_BY", "id")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE"))
        self.node_id = int(os.getenv("NODE_ID"))
        
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.worker_port = int(os.getenv("WORKER_PORT", "9000"))
        
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
        
        self.control = WorkerProtocol(self.health_server_ip, self.worker_port, self.health_server_port)
        self.control.listen()
    
    def callback(self, ch, method, properties, body):
        """
        Recibe un mensaje y lo envia al output_exchange, routeandolo
        segun routing_key = msg.id % number_of_nodes.
        """
        try:
            if self.running == False:
                # if self.input_rabbitmq.check_no_consumers():
                #     for i in range(self.number_of_nodes):
                #         self.output_rabbitmq.send_final(routing_key=str(i))
                self.input_rabbitmq.close_graceful(method)
                return
           
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet["client_id"]
            
            if is_final_packet(header):
                count = int(packet['count'])
                final, frequencies = self.control.send_final_count(client_id, count)
                if final:
                    freq_dict = {}
                    for pair in frequencies.split(","):
                        node_id, count = pair.split(":")
                        freq_dict[int(node_id)] = int(count)
                    for i in range(self.number_of_nodes):
                        self.output_rabbitmq.send_final(client_id=client_id, routing_key=str(i), count=freq_dict.get(i, 0))
                    self.control.delete_client(client_id)
                
                
                # Mando ack del final packet
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Deserializo la peli para obtener el id
            movie = packet.get("data")
            id = packet.get("id")
            movie_id = int(movie.get(self.router_by))

            # Calculo la routing key como el modulo entre el id y la cantidad de nodos
            
            routing_key = str(movie_id % self.number_of_nodes)
            # Routeo el mensaje segun el routing key
            self.output_rabbitmq.publish(packet_json, routing_key=routing_key)
            
            final, frequencies = self.control.insert_id(client_id, id, routing_key, self.node_id)
            if final:
                freq_dict = {}
                for pair in frequencies.split(","):
                    node_id, count = pair.split(":")
                    freq_dict[int(node_id)] = int(count)
                for i in range(self.number_of_nodes):
                    self.output_rabbitmq.send_final(client_id=client_id, routing_key=str(i), count=freq_dict.get(i, 0))
                self.control.delete_client(client_id)
                
            print(f" [✓] Sent movie with id: {movie_id} through the exchange using routing key: {routing_key}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)


    def start_node(self):
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in router node: {e}")
        finally:
            self.close()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.running = False
        if self.control:
            self.control.stop()
        if self.input_rabbitmq:
            self.input_rabbitmq.cancel_consumer()
    
    def close(self):
        print(f"Closing queues")
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
