import json
import os
import signal
import threading
from datetime import datetime
from common.middleware import Middleware
from common.storage_handler import StorageHandler
from common.leader_queue import LeaderQueue
from common.packet import DataPacket, handle_final_packet, is_final_packet

class JoinNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.router_buffer = {}  # Buffer temporal para emparejar por router
        self.running = True
        self.eof_main_by_client = {}  # EOF main por cliente
        self.storages_by_client = {}  # StorageHandler por cliente
        self.lock = threading.Lock()
        self.node_id = os.getenv("NODE_ID", "")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE", ""))
        self.input_queue_1 = f"{os.getenv('RABBITMQ_QUEUE_1', 'movie_queue_1')}_{self.node_id}"
        self.input_queue_2 = f"{os.getenv('RABBITMQ_QUEUE_2', 'movie_queue_2')}_{self.node_id}"
        self.exchange_1 = os.getenv("RABBITMQ_EXCHANGE_1", "")
        self.exchange_2 = os.getenv("RABBITMQ_EXCHANGE_2", "")
        self.consumer_tag = f"{os.getenv('RABBITMQ_CONSUMER_TAG', 'default_consumer')}_{self.node_id}"
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE", "default_final")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "")
        self.join_by = os.getenv("JOIN_BY", "id")
        
        self.keep_columns = None
        keep_columns = os.getenv("KEEP_COLUMNS", "")
        if keep_columns:
            self.keep_columns = [col.strip() for col in keep_columns.split(",") if col.strip()]
        
        self.threads = []
        
        if self.output_exchange: 
            self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.output_rabbitmq = Middleware(queue=self.output_queue)

        self.input_rabbitmq_1 = Middleware(
            queue=self.input_queue_1,
            consumer_tag=self.consumer_tag,
            exchange=self.exchange_1,
            publish_to_exchange=False,
            routing_key=self.node_id
        )
        self.input_rabbitmq_2 = Middleware(
            queue=self.input_queue_2,
            consumer_tag=self.consumer_tag,
            exchange=self.exchange_2,
            publish_to_exchange=False,
            routing_key=self.node_id
        )
        
        self.final_rabbitmq = Middleware(
            queue=self.final_queue,
            consumer_tag=self.consumer_tag,
            publish_to_exchange=False
        )
        
        self.leader_queue = None
        if int(self.node_id) == 0:
            self.leader_queue = LeaderQueue(self.final_queue, self.output_queue, self.consumer_tag, self.cluster_size)
        
        self.input_rabbitmq_map = {
            self.input_queue_1: self.input_rabbitmq_1,
            self.input_queue_2: self.input_rabbitmq_2,
        }
        
    def _get_storage_for_client(self, client_id):
        """Obtiene o crea un StorageHandler para un cliente."""
        with self.lock:
            if client_id not in self.storages_by_client:
                if client_id not in self.eof_main_by_client:
                    self.eof_main_by_client[client_id] = False
                storage_dir = f'./storage_{self.node_id}_{client_id}'
                self.storages_by_client[client_id] = StorageHandler(data_dir=storage_dir)
                print(f" [🆕] Creado StorageHandler para cliente '{client_id}' en '{storage_dir}'")
            return self.storages_by_client[client_id]

    def main_callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq_1.close_graceful(method)
                return
            
            packet_json = body.decode()
            header = json.loads(packet_json).get("header")

            if is_final_packet(header):
                print(f" [*] Cola '{self.input_queue_1}' terminó.")
                with self.lock:
                    self.eof_main_by_client[1] = True
                if handle_final_packet(method, self.input_rabbitmq_1):
                    self.input_rabbitmq_1.send_ack_and_close(method)
                return

            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            router = int(movie.get(self.join_by))

            if not router:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            with self.lock:
                if router not in self.router_buffer:
                    print(f" [🆕] Creating new router_buffer entry for router '{router}'")
                    self.router_buffer[router] = movie
                    print(f" [✅] Router '{router}' entry saved. Current buffer size: {len(self.router_buffer)}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def join_callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq_2.close_graceful(method)
                return
            
            packet_json = body.decode()
            header = json.loads(packet_json).get("header")

            if is_final_packet(header):
                print(f" [*] Cola '{self.input_queue_2}' terminó.")
                self.final_rabbitmq.send_final()
                if handle_final_packet(method, self.input_rabbitmq_2):
                    self.input_rabbitmq_2.send_ack_and_close(method)
                return

            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            router = int(movie.get(self.join_by))

            if not router:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Obtener el StorageHandler para el cliente
            storage = self._get_storage_for_client(1)
            
            if router in self.router_buffer:
                print(f" [🔍] Router '{router}' found in router_buffer")
                with self.lock:
                    movie1 = self.router_buffer[router]
                    joined_packet = self.create_joined_packet(movie1, movie)
                    self.output_rabbitmq.publish(joined_packet.to_json())
                    print(f" [✓] Joined and published router '{router}' to output_rabbitmq")
                    print(f" [✅] Router '{router}' entry remains in buffer. Current buffer size: {len(self.router_buffer)}")
            else:
                # Si eof_main es False, guardar en el disco
                if not self.eof_main_by_client.get(1, False):
                    print(f" [💾] Router '{router}' not in buffer, adding to disk")
                    storage.add(str(router), movie, group_key=self.input_queue_2)
                    print(f" [✅] Added router '{router}' to disk")
                    
            if self.eof_main_by_client.get(1, False):
                
                # Verificar si el disco está vacío
                stored_keys = storage.list_keys(group_key=self.input_queue_2)
                if not stored_keys:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                print(" [🔄] Iniciando merge completo (eof_main=True)")    
                # Realizar merge completo: combinar router_buffer con todos los datos del disco
                for key, _ in stored_keys:
                    router_key = int(key)  # Convertir la clave a entero
                    if router_key in self.router_buffer:
                        stored_movies = storage.retrieve(key, group_key=self.input_queue_2)
                        if stored_movies:
                            # Asegurarse de que stored_movies sea una lista
                            if not isinstance(stored_movies, list):
                                stored_movies = [stored_movies]
                            print(f" [🔍] Procesando router '{router_key}' con {len(stored_movies)} entradas en disco")
                            movie1 = self.router_buffer[router_key]
                            for movie2 in stored_movies:
                                joined_packet = self.create_joined_packet(movie1, movie2)
                                self.output_rabbitmq.publish(joined_packet.to_json())
                                print(f" [✓] Joined and published router '{router_key}' from disk to output_rabbitmq")
                    
                # Limpiar el disco después del merge
                print(f" [🧹] Limpiando disco para group_key '{self.input_queue_2}'")
                storage.remove_keys(group_key=self.input_queue_2)
                print(f" [✅] Disco limpio")
               

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            print(f" [! spectacles for the error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def create_joined_packet(self, movie1, movie2):
        combined_movie = {**movie1, **movie2}
        joined_packet = DataPacket(
            timestamp=datetime.utcnow().isoformat(),
            data=combined_movie,
            keep_columns=self.keep_columns,
        )
        return joined_packet
                
    def start_node(self):
        try:
            t1 = threading.Thread(target=self.input_rabbitmq_1.consume, args=(self.main_callback,))
            t1.start()
            self.threads.append(t1)
            t2 = threading.Thread(target=self.input_rabbitmq_2.consume, args=(self.join_callback,))
            t2.start()
            self.threads.append(t2)
            t1.join()
            t2.join()
            self.leader_queue.join()        
        except Exception as e:
            print(f" [!] Error in join node: {e}")
        finally:
            self.close()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()
    
    def close(self):
        print(f"Closing queues")
        self.running = False
        if self.leader_queue:
            self.leader_queue.close()
        self.input_rabbitmq_1.cancel_consumer()
        self.input_rabbitmq_2.cancel_consumer()
        self.input_rabbitmq_1.close()
        self.input_rabbitmq_2.close()
        self.final_rabbitmq.close()
        for client_id, storage in self.storages_by_client.items():
            print(f" [🧹] Limpiando almacenamiento para cliente '{client_id}'")
            storage.remove_keys()
        self.storages_by_client.clear()