import json
from common.middleware import Middleware
from common.packet import DataPacket, handle_final_packet, is_final_packet
from datetime import datetime
import os
import signal
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

        self.sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

    def callback(self, ch, method, properties, body):
        try:
            if self.running == False:
                if self.input_rabbitmq.check_no_consumers():
                    self.output_positive_rabbitmq.send_final()
                    self.output_negative_rabbitmq.send_final()
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibir paquete y mandar final packet si se recibe uno
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            
            if is_final_packet(header):
                # Si la lista de acks es None, entonces soy el primero en recibir el mensaje FIN
                # Agrego la lista con mi id y la reencolo
                if packet.get("acks") is None:
                    print(f"[Sentiment - FIN] - packet[acks] = None")
                    # Inicializo la lista acks con mi id
                    packet["acks"] = [self.node_id]
                    self.input_rabbitmq.publish(packet)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                # Si no estoy en la lista de ids, me agrego
                if not self.node_id in packet.get("acks"):
                    print(f"[Sentiment - FIN] - No estoy en la lista de acks")
                    packet["acks"] = packet["acks"] + [self.node_id]
                
                # Si todos los id estan en la lista de acks, mando final
                if len(packet["acks"]) == self.cluster_size:
                    client_id = packet["client_id"]
                    acks = packet["acks"]
                    print(f"[Sentiment - FIN] - Lista de acks completa ({acks}), mando final packet (client_id = {client_id})")
                    self.output_positive_rabbitmq.send_final(client_id=client_id)
                    self.output_negative_rabbitmq.send_final(client_id=client_id)
                
                # Si faltan ids en la lista de ids, reencolo el mensaje (despues de haberme agregado)
                else:
                    self.input_rabbitmq.publish(packet)
                # Mando ack del final packet
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            packet = DataPacket.from_json(packet_json)
            movie = packet.data

            # Procesar paquete (comunicarse con la lib de sentimientos)
            overview = movie.get('overview', '')
            if not isinstance(overview, str):
                overview = str(overview)
            sentiment = self.sentiment_analyzer(overview, truncation=True)[0]['label']
            movie['sentiment'] = sentiment

            filtered_packet = DataPacket(
                client_id=client_id,
                timestamp=datetime.utcnow().isoformat(),
                data=movie
            )

            # Publicar el paquete filtrado a la cola del gateway que corresponda
            if sentiment == "POSITIVE":
                self.output_positive_rabbitmq.publish(filtered_packet.to_json())
            elif sentiment == "NEGATIVE":
                self.output_negative_rabbitmq.publish(filtered_packet.to_json())
            else:
                print("[--------------] No es positivo ni negativo")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def start_node(self):
        print(f" [~] Starting sentiment analyzer")

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_positive_rabbitmq:
                self.output_positive_rabbitmq.close()
            if self.output_negative_rabbitmq:
                self.output_negative_rabbitmq.close()
    
    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()

    def close(self):
        print(f"Closing queues")
        self.running = False
        self.input_rabbitmq.cancel_consumer()
