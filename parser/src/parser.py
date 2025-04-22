import json
import pandas as pd
from io import StringIO
import uuid
from datetime import datetime
from common.middleware import Middleware

from common.packet import MoviePacket, DataPacket, handle_final_packet, is_final_packet, is_eof_packet
import os

MOVIES_FILE = "movies_metadata.csv"
RATINGS_FILE = "ratings.csv"
CREDITS_FILE = "credits.csv"

class ParserNode:
    def __init__(self):
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

        if self.exchange:  # <- si hay exchange, lo usamos
            self.input_rabbitmq = Middleware(
                queue=self.input_queue,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False
            )
        else:  # <- si no, conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
        
        # Load KEEP_MOVIES_COLUMNS
        keep_movies_columns_str = os.getenv("KEEP_MOVIES_COLUMNS", "")
        self.keep_movies_columns = [col.strip() for col in keep_movies_columns_str.split(",") if col.strip()]

        # Load KEEP_RATINGS_COLUMNS
        keep_ratings_columns_str = os.getenv("KEEP_RATINGS_COLUMNS", "")
        self.keep_ratings_columns = [col.strip() for col in keep_ratings_columns_str.split(",") if col.strip()]

        # Load KEEP_CREDITS_COLUMNS
        keep_credits_columns_str = os.getenv("KEEP_CREDITS_COLUMNS", "")
        self.keep_credits_columns = [col.strip() for col in keep_credits_columns_str.split(",") if col.strip()]

        # Load REPLACE
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
            # Parse message
            message = json.loads(body)
            header = message['header']
            
            if is_eof_packet(header):
                file = message['filename']
                self.output_rabbitmq.send_final(routing_key=file)
                return

            if is_final_packet(header):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            rows = message['rows']
            filename = message['filename']

            # Create CSV string
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
            df = df[[col for col in keep_columns if col in df.columns]]

            print(" [x] Received and processed CSV:")
            
            # Apply column renaming for each pair if the old column exists
            for old_name, new_name in self.rename_columns:
                if old_name in df.columns:
                    print(f" [~] Renaming '{old_name}' to '{new_name}'")
                    df = df.rename(columns={old_name: new_name})

            # Create a MoviePacket for each movie
            for _, row in df.iterrows():
                if filename == MOVIES_FILE:
                    movie = row.to_dict()
                    packet = DataPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data=movie
                    )
                elif filename == RATINGS_FILE:
                    rating = row.to_dict()
                    packet = DataPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data=rating
                    )
                elif filename == CREDITS_FILE:
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