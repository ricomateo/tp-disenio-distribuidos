import json
import pandas as pd
from io import StringIO
import uuid
from datetime import datetime
from common.middleware import Middleware
from common.packet import MoviePacket, handle_final_packet, is_final_packet
import os


class ParserNode:
    def __init__(self):
        self.keep_columns = []
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
        
        # Load KEEP_COLUMNS
        keep_columns_str = os.getenv("KEEP_COLUMNS", "")
        self.keep_columns = [col.strip() for col in keep_columns_str.split(",") if col.strip()]

    def callback(self, ch, method, properties, body):
        try:
            # Parse message
            message = json.loads(body)
            header = message['header']
            
            if is_final_packet(header):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            rows = message['rows']

            # Create CSV string
            csv_text = header + "\n" + "\n".join(rows)
            df = pd.read_csv(StringIO(csv_text))

            # Apply column filtering
            if self.keep_columns:
                print(f" [~] Conservando solo columnas: {self.keep_columns}")
                df = df[[col for col in self.keep_columns if col in df.columns]]
            else:
                print(" [!] No se especificaron columnas a mantener. Se usará el DataFrame completo.")

            print(" [x] Received and processed CSV:")
            print(df)
            
            # Create a MoviePacket for each movie
            for _, row in df.iterrows():
                movie = row.to_dict()
                packet = MoviePacket(
                    #packet_id=str(uuid.uuid4()),
                    timestamp=datetime.utcnow().isoformat(),
                    data={"source": self.input_queue},
                    movie=movie
                )
                self.output_rabbitmq.publish(packet.to_json())
                #print(f" [x] Published MoviePacket: {packet.packet_id} for movie: {movie.get('title', 'Unknown')}")
                
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
        if self.keep_columns:
            print(f" [~] Keeping columns: {self.keep_columns}")
      
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in parser node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()