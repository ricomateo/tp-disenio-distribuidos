import json
from io import StringIO
import signal
from datetime import datetime
import os
import logging
import pandas as pd
from common.logger import init_logging
from common.middleware import Middleware
from common.packet import DataPacket, is_delete_packet, is_final_packet
from common.worker_protocol import WorkerProtocol


init_logging(os.getenv("LOG_LEVEL"))


class ParserNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.keep_movies_columns = []
        self.keep_ratings_columns = []
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "")
        self.filename = os.getenv("FILENAME", "")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE"))
        self.node_id = int(os.getenv("NODE_ID"))
        
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.worker_port = int(os.getenv("WORKER_PORT", "9000"))
        
        self.keep_columns = None
        keep_columns = os.getenv("KEEP_COLUMNS", "")
        if keep_columns:
         self.keep_columns = [col.strip() for col in keep_columns.split(",") if col.strip()]
         
        self.input_rabbitmq = None
        if self.exchange:
            # Si hay exchange lo usamos
            self.input_rabbitmq = Middleware(
                queue=self.filename,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False,
                routing_key=self.filename
            )
        else:
            # Sino conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
        
        if self.output_exchange: 
            self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.output_rabbitmq = Middleware(queue=self.output_queue)

        # Cargo REPLACE
        replace_str = os.getenv("REPLACE", "")
        self.rename_columns = []
        if replace_str:
            try:
                for pair in replace_str.split(","):
                    old_name, new_name = pair.split(":")
                    self.rename_columns.append((old_name.strip(), new_name.strip()))
            except ValueError as e:
                logging.warning("Invalid REPLACE format: %s, error: %s. No columns will be renamed.", replace_str, e)
        
        self.control = WorkerProtocol(self.health_server_ip, self.worker_port, self.health_server_port)
        self.control.listen()
   
    def callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq.close_graceful(method)
                return
            # Parseo el mensaje
            packet = json.loads(body)
            header = packet['header']
            client_id = packet['client_id']
            
            if is_delete_packet(header):
                logging.info("Received DELETE packet for client %s", client_id)
                self.output_rabbitmq.send_delete(client_id=client_id, routing_key=self.filename)
                logging.info("Sent DELETE packet for client %s", client_id)
                self.control.delete_client(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if is_final_packet(header):
                logging.info("Received FINAL packet from client %s", client_id)
                count = int(packet['count'])
                final, count = self.control.send_final_count(client_id, count)
                if final:
                    self.output_rabbitmq.send_final(client_id=client_id, routing_key=self.filename, count=count)
                    logging.info("Sent FINAL packet for client %s", client_id)
                    self.control.delete_client(client_id)
                
                # Mando ack del final packet
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            rows = packet['rows']
            id = packet['id']

            # Creo el string CSV
            csv_text = header + "\n" + "\n".join(rows)
            df = pd.read_csv(StringIO(csv_text))
            
            logging.debug("Conservando solo columnas: %s", self.keep_columns)
            columns_to_keep = list(set(df.columns).intersection(self.keep_columns))
            df = df[columns_to_keep]
            df = df.dropna().copy()
            logging.debug("Received and processed CSV:")
            
            # Apply column renaming for each pair if the old column exists
            rename_dict = {old: new for old, new in self.rename_columns if old in df.columns}
            if rename_dict:
                df.rename(columns=rename_dict, inplace=True)

            row_count = 0
            for _, row in df.iterrows():
                packet = DataPacket(
                        client_id=client_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data=row.to_dict(),
                        id=f"{id}-{row_count}"
                )
                self.output_rabbitmq.publish(packet.to_json(), self.filename)
                row_count += 1
                
            final, count = self.control.insert_id(client_id, id, str(row_count))
            if final:
                self.output_rabbitmq.send_final(client_id=client_id, routing_key=self.filename, count=int(count))
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
        logging.info("Starting ParserNode: input_queue=%s, output_queue=%s", self.input_queue, self.output_queue)
        if self.keep_movies_columns:
            logging.info("Keeping movies columns: %s", self.keep_movies_columns)
        if self.keep_ratings_columns:
            logging.info("Keeping ratings columns: %s", self.keep_ratings_columns)
        if self.rename_columns:
            logging.info("Renaming columns: %s", self.rename_columns)
      
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            logging.error("Error in parser node: %s", e)
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
