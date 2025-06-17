import json
from common.middleware import Middleware
from common.packet import DataPacket, is_delete_packet, is_final_packet
from datetime import datetime
import os
import signal
from common.worker_protocol import WorkerProtocol
from transformers import pipeline

class SentimentNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.running = True
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "sentiment_queue")
        self.output_positive_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE_POSITIVE", "default_output")
        self.output_negative_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE_NEGATIVE", "default_output")

        self.exchange = os.getenv("RABBITMQ_EXCHANGE")
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "sentiment_consumer")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE"))
        self.node_id = int(os.getenv("NODE_ID"))
        
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.worker_port = int(os.getenv("WORKER_PORT", "9000"))
        self.input_rabbitmq = None
        
        if self.exchange:  
            self.input_rabbitmq = Middleware(
                queue=self.input_queue,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False,
                routing_key=self.routing_key
            )
        else:  
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
            

        self.output_positive_rabbitmq = Middleware(queue=self.output_positive_queue)
        self.output_negative_rabbitmq = Middleware(queue=self.output_negative_queue)
        
        print("listen")
        self.control = WorkerProtocol(self.health_server_ip, self.worker_port, self.health_server_port)
        self.control.listen()
        print("listened")
        
        self.sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        
        

    def callback(self, ch, method, properties, body):
        try:
            if self.running == False:
                #self.output_positive_rabbitmq.send_final()
                #self.output_negative_rabbitmq.send_final()
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibir paquete y mandar final packet si se recibe uno
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            
            if is_delete_packet(header):
                self.output_positive_rabbitmq.send_delete(client_id=client_id)
                self.output_negative_rabbitmq.send_delete(client_id=client_id)
                self.control.delete_client(client_id)
            
            if is_final_packet(header):
                
                count = int(packet['count'])
                final, frequencies = self.control.send_final_count(client_id, count)
                
                if final:
                    freq_dict = {}
                    for pair in frequencies.split(","):
                        node_id, count = pair.split(":")
                        freq_dict[int(node_id)] = int(count)
                    self.output_positive_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(0, 0))
                    self.output_negative_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(1, 0))
                    self.control.delete_client(client_id)
                    
                # Mando ack del final packet
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            
            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            id = packet.id

            # Procesar paquete (comunicarse con la lib de sentimientos)
            overview = movie.get('overview', '')
            if not isinstance(overview, str):
                overview = str(overview)
            sentiment = self.sentiment_analyzer(overview, truncation=True)[0]['label']
            movie['sentiment'] = sentiment

            filtered_packet = DataPacket(
                client_id=client_id,
                timestamp=datetime.utcnow().isoformat(),
                data=movie,
                id=id
            )

            # Publicar el paquete filtrado a la cola del gateway que corresponda
            if sentiment == "POSITIVE":
                self.output_positive_rabbitmq.publish(filtered_packet.to_json())
                      
            elif sentiment == "NEGATIVE":
                self.output_negative_rabbitmq.publish(filtered_packet.to_json())
            else:
                print("[--------------] No es positivo ni negativo")
                
            sentiment_code = "0" if sentiment == "POSITIVE" else "1" 
               
            final, frequencies = self.control.insert_id(client_id, id, sentiment_code)
            if final:
                freq_dict = {}
                for pair in frequencies.split(","):
                    node_id, count = pair.split(":")
                    freq_dict[int(node_id)] = int(count)
                self.output_positive_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(0, 0))
                self.output_negative_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(1, 0))
                self.control.delete_client(client_id)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self):
        print(f" [~] Starting sentiment analyzer")

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
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
        if self.output_positive_rabbitmq:
            self.output_positive_rabbitmq.close()
        if self.output_negative_rabbitmq:
            self.output_negative_rabbitmq.close()