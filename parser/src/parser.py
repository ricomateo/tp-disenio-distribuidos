import json
import pandas as pd
from io import StringIO
import signal
from datetime import datetime
from common.middleware import Middleware

from common.packet import DataPacket, is_final_packet

import os

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
                print(f" [~] Invalid REPLACE format: {replace_str}, error: {e}. No columns will be renamed.")
   
    def callback(self, ch, method, properties, body):


            try:
                # TODO: ver si hay que cambiar esto
                if self.running == False:
                    # if self.input_rabbitmq.check_no_consumers():
                    #     self.output_rabbitmq.send_final(routing_key=self.filename)
                    self.input_rabbitmq.close_graceful(method)
                    return
                # Parseo el mensaje
                packet = json.loads(body)
                header = packet['header']

                if is_final_packet(header):
                        # Si la lista de acks es None, entonces soy el primero en recibir el mensaje FIN
                    # Agrego la lista con mi id y la reencolo
                    if packet.get("acks") is None:
                        print(f"[Parser - FIN] - packet[acks] = None, packet = {packet}")
                        # Inicializo la lista acks con mi id
                        packet["acks"] = []
                
                    # Si no estoy en la lista de ids, me agrego
                    if not self.node_id in packet.get("acks"):
                        print(f"[Parser - FIN] - No estoy en la lista de acks")
                        packet["acks"] = packet["acks"] + [self.node_id]
                    
                    # Si todos los id estan en la lista de acks, mando final
                    if len(packet["acks"]) == self.cluster_size:
                        client_id = packet["client_id"]
                        acks = packet["acks"]
                        print(f"[Parser - FIN] - Lista de acks completa ({acks}), mando final packet (client_id = {client_id})")
                        self.output_rabbitmq.send_final(client_id=client_id, routing_key=self.filename)
                    
                    # Si faltan ids en la lista de ids, reencolo el mensaje (despues de haberme agregado)
                    else:
                        print(f"[Parser - FIN] - Ya estoy en la lista pero faltan IDs, reencolo")
                        self.input_rabbitmq.publish(packet)
                    # Mando ack del final packet
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                
                rows = packet['rows']
                client_id = packet['client_id']

                # Creo el string CSV
                csv_text = header + "\n" + "\n".join(rows)
                df = pd.read_csv(StringIO(csv_text))
                
                print(f" [~] Conservando solo columnas: {self.keep_columns}")
                columns_to_keep = list(set(df.columns).intersection(self.keep_columns))
                df = df[columns_to_keep]
                df = df.dropna().copy()
                print(" [x] Received and processed CSV:")
                
                # Apply column renaming for each pair if the old column exists
                rename_dict = {old: new for old, new in self.rename_columns if old in df.columns}
                if rename_dict:
                    df.rename(columns=rename_dict, inplace=True)

                for _, row in df.iterrows():
                    packet = DataPacket(
                            client_id=client_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=row.to_dict()
                    )
                    self.output_rabbitmq.publish(packet.to_json(), self.filename)
                    
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
            self.close()
           
    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.running = False
        self.input_rabbitmq.cancel_consumer()
    
    def close(self):
        print(f"Closing queues")
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
    
