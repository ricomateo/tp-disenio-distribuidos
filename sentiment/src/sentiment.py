# filter.py
import json
import math
from common.middleware import Middleware
from common.packet import MoviePacket, handle_final_packet, is_final_packet
from datetime import datetime
import os

from transformers import pipeline

class SentimentNode:
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "movie_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")

        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
        self.output_rabbitmq = Middleware(queue=self.output_queue)

    def callback(self, ch, method, properties, body):
        try:
            # Recibir paquete
            packet_json = body.decode()
            
            if is_final_packet(json.loads(packet_json).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            packet = MoviePacket.from_json(packet_json)
            movie = packet.movie

            # Procesar paquete (comunicarse con la lib de sentimientos)

            sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
            packet['sentiment'] = packet['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])

            filtered_packet = MoviePacket(
                #packet_id=packet.packet_id,
                timestamp=datetime.utcnow().isoformat(),
                data={"source": "sentiment_node"},
                movie=movie
            )

            # Publicar el paquete filtrado a la cola del gateway
           
            self.output_rabbitmq.publish(filtered_packet.to_json())
            
            print(f" [✓] Filtered and Published to {self.output_queue}: Title: {movie.get('title', 'Unknown')}, Genres: {movie.get('genres')}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def start_node(self, filters):
        self.filters = filters
        print(f" [~] Applying filters: {self.filters}")

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()
