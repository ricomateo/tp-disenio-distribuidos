import json
from common.middleware import Middleware
from common.packet import DataPacket, handle_final_packet, is_final_packet
from src.check_condition import check_condition
from datetime import datetime
import os
import signal

class FilterNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.filters = {}
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "movie_queue")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "") 
        self.keep_columns = None
        keep_columns = os.getenv("KEEP_COLUMNS", "")
        if keep_columns:
         self.keep_columns = [col.strip() for col in keep_columns.split(",") if col.strip()]
         
        if self.output_exchange: 
            self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.output_rabbitmq = Middleware(queue=self.output_queue)
        
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

    def callback(self, ch, method, properties, body):
        try:
            packet_json = body.decode()
            if is_final_packet(json.loads(packet_json).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return

            packet = DataPacket.from_json(packet_json)
            movie = packet.data

            # Aplicar los filtros de la instancia
            for _, condition in self.filters.items():
                _, _, key = condition
                value = movie.get(key)
                if not check_condition(value, condition):
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            filtered_packet = DataPacket(
                timestamp=datetime.utcnow().isoformat(),
                data=movie,
                keep_columns=self.keep_columns
            )

            # Publicar el paquete filtrado a la cola del gateway
            
            self.output_rabbitmq.publish(filtered_packet.to_json())
            
            print(f" [✓] Filtered and Published to {self.output_queue}: ID: {movie.get('id')}, Title: {movie.get('title', 'Unknown')}, Genres: {movie.get('genres')}")
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

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()
    
    def close(self):
        print(f"Closing queues")
        self.input_rabbitmq.close()
        self.output_rabbitmq.close()
