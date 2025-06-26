import json
import os
import signal
import logging
from common.logger import init_logging
from transformers import pipeline
from datetime import datetime
from common.middleware import Middleware
from common.packet import DataPacket, is_delete_packet, is_final_packet
from common.worker_protocol import WorkerProtocol

init_logging(os.getenv("LOG_LEVEL", "info"))

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

        self.control = WorkerProtocol(self.health_server_ip, self.worker_port, self.health_server_port)
        self.control.listen()

        self.sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')


    def callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibir paquete y mandar final packet si se recibe uno
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            
            if is_delete_packet(header):
                logging.info("Receive DELETE packet from client %s", client_id)
                self.output_positive_rabbitmq.send_delete(client_id=client_id)
                self.output_negative_rabbitmq.send_delete(client_id=client_id)
                logging.info("Sent DELETE packet for client %s to both output queues", client_id)
                self.control.delete_client(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if is_final_packet(header):
                logging.info("Received FINAL packet from client %s", client_id)
                count = int(packet['count'])
                final, frequencies = self.control.send_final_count(client_id, count)
                
                if final:
                    freq_dict = {}
                    for pair in frequencies.split(","):
                        node_id, count = pair.split(":")
                        freq_dict[int(node_id)] = int(count)
                    self.output_positive_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(0, 0))
                    self.output_negative_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(1, 0))
                    logging.info("Sent the FINAL packet for client %s to both output queues (positive and negative)", client_id)
                    self.control.delete_client(client_id)
                else:
                    logging.info("Missing packets from client %s to reach the final count", client_id)
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
                logging.warning("Packet sentiment is not POSITIVE nor NEGATIVE")

            sentiment_code = "0" if sentiment == "POSITIVE" else "1" 

            final, frequencies = self.control.insert_id(client_id, id, sentiment_code)
            if final:
                logging.info("Received packet from client %s is the last one.", client_id)
                freq_dict = {}
                for pair in frequencies.split(","):
                    node_id, count = pair.split(":")
                    freq_dict[int(node_id)] = int(count)
                self.output_positive_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(0, 0))
                self.output_negative_rabbitmq.send_final(client_id=client_id, count=freq_dict.get(1, 0))
                self.control.delete_client(client_id)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.debug("Message %s acknowledged", method.delivery_tag)

        except json.JSONDecodeError as e:
            logging.warning("Error decoding JSON: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            logging.warning("Error processing message: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self):
        logging.info("Starting sentiment analyzer")

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            logging.error("Error in filter node: %s", e)
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
        if self.output_positive_rabbitmq:
            self.output_positive_rabbitmq.close()
        if self.output_negative_rabbitmq:
            self.output_negative_rabbitmq.close()
