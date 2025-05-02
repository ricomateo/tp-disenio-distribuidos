import json
from common.middleware import Middleware
from common.packet import DataPacket, handle_final_packet, is_final_packet
import threading 
from datetime import datetime
import os
import signal



class JoinNode:
    def __init__(self):

        signal.signal(signal.SIGTERM, self._sigterm_handler)
        # Buffer temporal para emparejar por router
        self.router_buffer = {}  

        self.running = True

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

        
        self.keep_columns = None
        keep_columns = os.getenv("KEEP_COLUMNS", "")
        if keep_columns:
         self.keep_columns = [col.strip() for col in keep_columns.split(",") if col.strip()]
         

        self.threads = []

        
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
                if self.running == False:
                    rabbitmq_instance = self.input_rabbitmq_map[source_name]
                    rabbitmq_instance.close_graceful(method)
                    return
            
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

                
                    #print(f" [🔒] Acquired lock for processing message from '{source_name}' with router '{router}'")
                    
                if source_name == self.input_queue_1:
                  with self.lock:
                        #print(f" [📥] Processing message from queue_1 (source: {source_name})")
                        # Store queue_1 messages in router_buffer
                        if router not in self.router_buffer:
                            print(f" [🆕] Creating new router_buffer entry for router '{router}'")
                            self.router_buffer[router] = movie
                            print(f" [✅] Router '{router}' entry saved. Current buffer size: {len(self.router_buffer)}")
                   
                    
                elif source_name == self.input_queue_2:
       
                   with self.lock:     
                        #print(f" [📥] Processing message from queue_2 (source: {source_name})")
                         # For queue_2, check for match without storing
                        if router in self.router_buffer:
                                print(f" [🔍] Router '{router}' found in router_buffer")
                                movie1 = self.router_buffer[router]
                                #print(f" [🔗] Preparing to join movie1 (from queue_1) and movie2 (from queue_2) for router '{router}'")
                                
                                joined_packet = self.create_joined_packet(movie1, movie)
                                #print(f" [📦] Created joined packet for router '{router}'")
                                
                                self.output_rabbitmq.publish(joined_packet.to_json())
                                print(f" [✓] Joined and published router '{router}' to output_rabbitmq")
                                
                                # Remove matched queue_1 entry
                                #print(f" [🗑️] Removing router_buffer entry for router '{router}'")
                                #print(f" [✅] Router '{router}' entry removed. Current buffer size: {len(self.router_buffer)}")
                           

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except json.JSONDecodeError as e:
                print(f" [!] Error decoding JSON: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
            except Exception as e:
                print(f" [!] Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

        return callback
    
    def create_joined_packet(self, movie1, movie2):
        # Combinar los diccionarios movie1 y movie2
        combined_movie = {**movie1, **movie2}
        
        joined_packet = DataPacket(
            timestamp=datetime.utcnow().isoformat(),
            data=combined_movie,
            keep_columns=self.keep_columns,
        )
        return joined_packet

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
            if int(self.node_id) == 0:
             self.final_rabbitmq.send_final()
            t3 = threading.Thread(target=self.final_rabbitmq.consume, args=(self.noop_callback,))
            
            t1.start()
            t3.start()

            self.threads.append(t1)
            self.threads.append(t3)

            t1.join()
            t2 = threading.Thread(target=self.input_rabbitmq_2.consume, args=(self.make_callback(self.input_queue_2),))
            self.threads.append(t2)
            t2.start()
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

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()
    
    def close(self):
        print(f"Closing queues")
        self.running = False
        self.finished_event.set()
        self.input_rabbitmq_1.cancel_consumer()
        self.input_rabbitmq_2.cancel_consumer()
        self.final_rabbitmq.cancel_consumer()
        #self.final_rabbitmq.close()
