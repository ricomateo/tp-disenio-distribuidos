import json
import pandas as pd
from io import StringIO
import signal
from datetime import datetime
from common.middleware import Middleware
import time
from common.packet import DataPacket, handle_final_packet, is_final_packet, is_eof_packet
import os

MOVIES_FILE = "movies_metadata.csv"
RATINGS_FILE = "ratings.csv"
CREDITS_FILE = "credits.csv"

class ParserNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.keep_movies_columns = []
        self.keep_ratings_columns = []
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "")

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
                publish_to_exchange=False
            )
        else:
            # Sino conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
        
        # Cargo KEEP_MOVIES_COLUMNS
        keep_movies_columns_str = os.getenv("KEEP_MOVIES_COLUMNS", "")
        self.keep_movies_columns = [col.strip() for col in keep_movies_columns_str.split(",") if col.strip()]

        # Cargo KEEP_RATINGS_COLUMNS
        keep_ratings_columns_str = os.getenv("KEEP_RATINGS_COLUMNS", "")
        self.keep_ratings_columns = [col.strip() for col in keep_ratings_columns_str.split(",") if col.strip()]

        # Cargo KEEP_CREDITS_COLUMNS
        keep_credits_columns_str = os.getenv("KEEP_CREDITS_COLUMNS", "")
        self.keep_credits_columns = [col.strip() for col in keep_credits_columns_str.split(",") if col.strip()]

        # Cargo REPLACE
        replace_str = os.getenv("REPLACE", "")
        self.rename_columns = []
        if replace_str:
            try:
                for pair in replace_str.split(","):
                    old_name, new_name = pair.split(":")
                    self.rename_columns.append((old_name.strip(), new_name.strip()))
            except ValueError as e:
                print(f" [~] Invalid REPLACE format: {replace_str}, error: {e}. No columns will be renamed.")

    def callback(self, ch, method, properties, body):
        try:
            # Parseo el mensaje
            message = json.loads(body)
            header = message['header']
            
            if is_eof_packet(header):
                file = message['filename']
                time.sleep(5)
                self.output_rabbitmq.send_final(routing_key=file)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if is_final_packet(header):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            rows = message['rows']
            filename = message['filename']

            # Creo el string CSV
            csv_text = header + "\n" + "\n".join(rows)
            df = pd.read_csv(StringIO(csv_text))

            if filename == MOVIES_FILE:
                keep_columns = self.keep_movies_columns
            elif filename == RATINGS_FILE:
                keep_columns = self.keep_ratings_columns
            elif filename == CREDITS_FILE:
                keep_columns = self.keep_credits_columns
            else:
                print(f"[x] Unknown filename: {filename}. Dropping the message...")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
                return
            
            print(f" [~] Conservando solo columnas: {keep_columns}")
            columns_to_keep = list(set(df.columns).intersection(keep_columns))
            df = df[columns_to_keep]

            print(" [x] Received and processed CSV:")
            

            # Apply column renaming for each pair if the old column exists
            rename_dict = {old: new for old, new in self.rename_columns if old in df.columns}
            if rename_dict:
                df.rename(columns=rename_dict, inplace=True)


            
            for _, row in df.iterrows():
                packet = DataPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data=row.to_dict()
                )
                routing_key = filename
                self.output_rabbitmq.publish(packet.to_json(), routing_key)

                
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")
        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self):
        print(f" [~] Starting ParserNode: input_queue={self.input_queue}, output_queue={self.output_queue}")
        if self.keep_movies_columns:
            print(f" [~] Keeping movies columns: {self.keep_movies_columns}")
        if self.keep_ratings_columns:
            print(f" [~] Keeping ratings columns: {self.keep_ratings_columns}")
        if self.rename_columns:
            print(f" [~] Renaming columns: {self.rename_columns}")
      
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in parser node: {e}")
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
