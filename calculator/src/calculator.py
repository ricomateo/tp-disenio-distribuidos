import json
import threading
from common.leader_queue import LeaderQueue
from common.middleware import Middleware
from common.packet import DataPacket, is_final_packet
from datetime import datetime
import os
import signal
from src.calculation import Calculation
import math

class CalculatorNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.node_id = os.getenv("NODE_ID")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE", ""))
        self.finished_event = threading.Event()
        base_queue = os.getenv('RABBITMQ_QUEUE', 'movie_queue_1')
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.consumer_tag = f"{os.getenv('RABBITMQ_CONSUMER_TAG', 'default_consumer')}_{self.node_id}"
        self.exchange = os.getenv("RABBITMQ_EXCHANGE")
        self.operation = os.getenv("OPERATION", "")
        self.output_rabbitmq = Middleware(queue=self.output_queue)
        self.input_queue = f"{base_queue}_{self.node_id}" if self.exchange else base_queue
        self.routing_key = os.getenv("ROUTING_KEY") or self.node_id
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE")
        self.calculator = Calculation(self.operation, self.input_queue)
        self.final_rabbitmq = None
        self.threads = []
        
        self.leader_queue = None
        if int(self.node_id) == 0:
            self.leader_queue = LeaderQueue(self.final_queue, self.output_queue, self.consumer_tag, self.cluster_size)
        
        if self.final_queue:
            self.final_rabbitmq = Middleware(
            queue=self.final_queue,
            consumer_tag=self.consumer_tag,
            publish_to_exchange=False
        )
        
        if self.exchange:  # <- si hay exchange, lo usamos
            self.input_rabbitmq = Middleware(
                queue=self.input_queue,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False,
                routing_key=self.routing_key
            )
        else:  # <- si no, conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)


    def callback(self, ch, method, properties, body):
        try:
            if self.running == False:
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibo el paquete y en caso de ser el ultimo, mando los datos y el final packet
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            if header and is_final_packet(header):
                client_id = packet.get("client_id") 
                results = self.calculator.get_result(client_id)
                
                if self.operation == "ratio_by:revenue,budget":
                    # Si la lista de acks es None, entonces soy el primero en recibir el mensaje FIN
                    # Inicializo una lista vacia y reencolo el mensaje
                    if packet.get("acks") is None:
                        print(f"[Calculator - FIN] - packet[acks] = None")
                        # Inicializo la lista acks vacia
                        packet["acks"] = []

                    # Si no estoy en la lista de ids, me agrego, mando los resultados y mando el mensaje final
                    if not self.node_id in packet.get("acks"):
                        print(f"[Calculator - FIN] - No estoy en la lista de acks")
                        packet["acks"] = packet["acks"] + [self.node_id]
                        for result in results:
                            print("Resultados del cálculo:", result)
                            data_packet = DataPacket(
                                client_id=client_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={
                                    "source": f"calculator_{self.operation}",
                                    **result
                                }
                            )
                            self.output_rabbitmq.publish(data_packet.to_json())
                        self.final_rabbitmq.send_final(client_id=client_id)
                    
                    # Si faltan IDs en la lista de acks, reencolo
                    if len(packet["acks"]) < math.ceil(self.cluster_size / 2):
                        client_id = packet["client_id"]
                        acks = packet["acks"]
                        print(f"[Calculator - FIN] - Faltan IDs ({acks}), reencolo (client_id = {client_id})")
                        # Reencolo
                        self.input_rabbitmq.publish(packet)
                    
                    # Mando ACK
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                else:
                    for result in results:
                        print("Resultados del cálculo:", result)
                        data_packet = DataPacket(
                            client_id=client_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data={
                                "source": f"calculator_{self.operation}",
                                **result
                            }
                        )
                        self.output_rabbitmq.publish(data_packet.to_json())
                    self.final_rabbitmq.send_final(client_id=client_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            client_id = packet.client_id
            # Process movie using calculator
            success = self.calculator.process_movie(client_id, movie)
            
            if success:
                print(f"[input - {self.input_queue}] Processed movie: {movie.get('title', 'Unknown')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f" [x] Message {method.delivery_tag} acknowledged")
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)


        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}, raw packet is {packet_json}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self): 
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in calculator node: {e}")
        finally:
            if self.leader_queue:
                self.leader_queue.join()
            self.close()
            
   
    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.running = False
        self.final_rabbitmq.cancel_consumer()
        self.input_rabbitmq.cancel_consumer()
        if self.leader_queue:
            self.leader_queue.close()

    def close(self):
        print(f"Closing queues")
        if self.leader_queue:
            self.leader_queue.close()
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
        
       