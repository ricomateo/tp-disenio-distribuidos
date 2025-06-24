import json
import logging
from datetime import datetime
import os
import signal
from src.check_condition import check_condition
from common.logger import init_logging
from common.middleware import Middleware
from common.worker_protocol import WorkerProtocol
from common.packet import DataPacket, is_delete_packet, is_final_packet

init_logging(os.getenv("LOG_LEVEL", "info"))

class FilterNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.filters = {}
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "movie_queue")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.worker_port = int(os.getenv("WORKER_PORT", "9000"))
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "") 
        self.keep_columns = None
        keep_columns = os.getenv("KEEP_COLUMNS", "")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE"))
        self.node_id = int(os.getenv("NODE_ID"))


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
            
        self.control = WorkerProtocol(self.health_server_ip, self.worker_port, self.health_server_port)
        self.control.listen()
        
    def callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq.close_graceful(method)
                return

            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            
            if is_delete_packet(header):
                logging.info("Received DELETE packet for client %s", client_id)
                self.output_rabbitmq.send_delete(client_id=client_id)
                logging.info("Sent DELETE packet for client %s", client_id)
                self.control.delete_client(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if is_final_packet(header):
                logging.info("Received FINAL packet from client %s", client_id)
                count = int(packet['count'])
                final, count = self.control.send_final_count(client_id, count)
                if final:
                    self.output_rabbitmq.send_final(client_id=client_id, count=count)
                    logging.info("Sent FINAL packet for client %s", client_id)
                    self.control.delete_client(client_id)
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            movie = packet.get("data")
            id = packet.get("id")

            # Aplicar los filtros de la instancia
            for _, condition in self.filters.items():
                _, _, key = condition
                value = movie.get(key)
                if not check_condition(value, condition):
                    final, count = self.control.insert_id(client_id, id, "0")
                    if final:
                        self.output_rabbitmq.send_final(client_id=client_id, count=count)
                        self.control.delete_client(client_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            filtered_packet = DataPacket(
                client_id=client_id,
                timestamp=datetime.utcnow().isoformat(),
                data=movie,
                id=id,
                keep_columns=self.keep_columns
            )
            # Publicar el paquete filtrado a la cola del gateway
            self.output_rabbitmq.publish(filtered_packet.to_json())

            final, count = self.control.insert_id(client_id, id, "1")
            if final:
                self.output_rabbitmq.send_final(client_id=client_id, count=count)
                self.control.delete_client(client_id)
            
            
            logging.debug(
                "Filtered and Published to %s: ID: %s, Title: %s , Genres: %s",
                self.output_queue, movie.get('id'), movie.get('title', 'Unknown'), movie.get('genres')
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.debug("Message %s acknowledged", method.delivery_tag)

        except json.JSONDecodeError as e:
            logging.warning("Error decoding JSON: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            logging.warning("Error processing message: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self, filters):
        self.filters = filters
        logging.info("Applying filters: %s", self.filters)

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            logging.warning("Error in filter node: %s", e)
        finally:
            self.close()
            

    def _sigterm_handler(self, signum, _):
        logging.info("Received SIGTERM signal")
        self.running = False
        if self.control:
            self.control.stop()
        if self.input_rabbitmq:
            self.input_rabbitmq.cancel_consumer()
    
    def close(self):
        logging.info("Closing queues")
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()    
        

