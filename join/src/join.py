# filter.py
import json
from common.middleware import Middleware
from common.packet import DataPacket, MoviePacket, handle_final_packet, is_final_packet
import threading 
from datetime import datetime
import os
import time

def create_joined_packet(movie1, movie2, router):
    # Combinar los diccionarios movie1 y movie2
    combined_movie = {**movie1, **movie2}
    
    joined_packet = DataPacket(
        timestamp=datetime.utcnow().isoformat(),
        data=combined_movie
    )
    return joined_packet

class JoinNode:
    def __init__(self):
        # Buffer temporal para emparejar por router
        self.router_buffer = {}  

        self.lock = threading.Lock()
        self.finished_event = threading.Event()
        self.node_id = os.getenv("NODE_ID", "")
        self.input_queue_1 = f"{os.getenv('RABBITMQ_QUEUE_1', 'movie_queue_1')}_{self.node_id}"
        self.input_queue_2 = f"{os.getenv('RABBITMQ_QUEUE_2', 'movie_queue_2')}_{self.node_id}"
        self.exchange_1 = os.getenv("RABBITMQ_EXCHANGE_1", "")
        self.exchange_2 = os.getenv("RABBITMQ_EXCHANGE_2", "")
        self.consumer_tag = f"{os.getenv('RABBITMQ_CONSUMER_TAG', 'default_consumer')}_{self.node_id}"
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE", "default_final")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "")
        self.join_by = os.getenv("JOIN_BY", "id")
        
        if self.output_exchange: 
            self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.output_rabbitmq = Middleware(queue=self.output_queue)

        self.input_rabbitmq_1 = Middleware(
            queue=self.input_queue_1,
            consumer_tag=self.consumer_tag,
            exchange=self.exchange_1,
            publish_to_exchange=False,
            routing_key=self.node_id
        )
        self.input_rabbitmq_2 = Middleware(
            queue=self.input_queue_2,
            consumer_tag=self.consumer_tag,
            exchange=self.exchange_2,
            publish_to_exchange=False,
            routing_key=self.node_id
        )
        
        self.final_rabbitmq = Middleware(
            queue=self.final_queue,
            consumer_tag=self.consumer_tag,
            publish_to_exchange=False
        )
        
        self.input_rabbitmq_map = {
            self.input_queue_1: self.input_rabbitmq_1,
            self.input_queue_2: self.input_rabbitmq_2,
        }

    def make_callback(self, source_name):
        def callback(ch, method, properties, body):
            try:
                packet_json = body.decode()
                header = json.loads(packet_json).get("header")

                if is_final_packet(header):
                    print(f" [*] Cola '{source_name}' terminó.")
                    rabbitmq_instance = self.input_rabbitmq_map[source_name]
                    if handle_final_packet(method, rabbitmq_instance):
                        rabbitmq_instance.send_ack_and_close(method)
                    return

                packet = DataPacket.from_json(packet_json)
                movie = packet.data
                router = int(movie.get(self.join_by))

                if not router:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                with self.lock:
                    if router not in self.router_buffer:
                        self.router_buffer[router] = {}

                    self.router_buffer[router][source_name] = movie

                    if (
                        self.input_queue_1 in self.router_buffer[router] and 
                        self.input_queue_2 in self.router_buffer[router]
                    ):
                        movie1 = self.router_buffer[router][self.input_queue_1]
                        movie2 = self.router_buffer[router][self.input_queue_2]

                        joined_packet = create_joined_packet(movie1, movie2, router)

                        self.output_rabbitmq.publish(joined_packet.to_json())
                        print(f" [✓] Joined and published router '{router}'")
                        del self.router_buffer[router]

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except json.JSONDecodeError as e:
                print(f" [!] Error decoding JSON: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
            except Exception as e:
                print(f" [!] Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

        return callback

    def noop_callback(self, ch, method, properties, body):
        # Si ambas terminaron, ahora sí mando el final al siguiente nodo
        packet_json = body.decode()
        header = json.loads(packet_json).get("header")
     
        self.finished_event.wait()
        
        if is_final_packet(header):
            print(f" [!] Final rabbitmq stop consuming.")
            if handle_final_packet(method, self.final_rabbitmq):
                self.output_rabbitmq.send_final()
                self.final_rabbitmq.send_ack_and_close(method)
                print(f" [!] Final rabbitmq send final.")
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)
                
    def start_node(self):
        try:
            t1 = threading.Thread(target=self.input_rabbitmq_1.consume, args=(self.make_callback(self.input_queue_1),))
            t2 = threading.Thread(target=self.input_rabbitmq_2.consume, args=(self.make_callback(self.input_queue_2),))
            if int(self.node_id) == 0:
             self.final_rabbitmq.send_final()
            t3 = threading.Thread(target=self.final_rabbitmq.consume, args=(self.noop_callback,))
            
            t1.start()
            t2.start()
            t3.start()

            t1.join()
            t2.join()
            self.finished_event.set()
            t3.join()
                    
        except Exception as e:
            print(f" [!] Error in join node: {e}")
        finally:
            if self.input_rabbitmq_1:
                self.input_rabbitmq_1.close()
            if self.input_rabbitmq_2:
                self.input_rabbitmq_2.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()
            if self.final_rabbitmq:
                self.final_rabbitmq.close()
