import json
import threading
from common.middleware import Middleware
from common.packet import DataPacket, handle_final_packet, is_final_packet
from datetime import datetime
import os
import signal
from src.calculation import Calculation

class CalculatorNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.node_id = os.getenv("NODE_ID")
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
            # Recibo el paquete y en caso de ser el ultimo, mando los datos y el final packet
            packet_json = body.decode()
            
            header = json.loads(packet_json).get("header")
            if header and is_final_packet(header):    
                results = self.calculator.get_result()
                
                for result in results:
                    print("Resultados del cálculo:", result)
                    data_packet = DataPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data={
                            "source": f"calculator_{self.operation}",
                            **result
                        }
                    )
                    self.output_rabbitmq.publish(data_packet.to_json())  
                         
                if handle_final_packet(method, self.input_rabbitmq):
                    if self.final_rabbitmq == None:
                     self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            # Process movie using calculator
            success = self.calculator.process_movie(movie)
            
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
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def start_node(self):
        print(f" [~] Starting sentiment analyzer")
        if self.final_rabbitmq:
            t3 = threading.Thread(target=self.final_rabbitmq.consume, args=(self.noop_callback,))
            if int(self.node_id) == 0:
                self.final_rabbitmq.send_final()  
            t3.start()
            self.threads.append(t3)
            
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
        finally:
            if self.final_rabbitmq:
                self.finished_event.set()
                for thread in self.threads:
                    thread.join()
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()
   
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
                

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()

    def close(self):
        print(f"Closing queues")
        self.finished_event.set()
        self.input_rabbitmq.close()
        self.output_rabbitmq.close()
        for thread in self.threads:
            thread.join()
        if self.final_rabbitmq is not None:
            self.final_rabbitmq.close()
