import json
import os
import signal
import threading
from datetime import datetime
from common.middleware import Middleware
from common.storage_handler import StorageHandler
from common.leader_queue import LeaderQueue
from common.packet import DataPacket, is_delete_packet, is_final_packet
from common.worker_protocol import WorkerProtocol

class JoinNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.router_buffer_by_client = {}  # Buffer temporal para emparejar por router
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
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.join_by = os.getenv("JOIN_BY", "id")
        self.count_by_client = {}
        
        self.count_test = 0

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

        self.control = WorkerProtocol(self.health_server_ip, self.health_server_port, self.health_server_port)

    def _get_storage_for_client(self, client_id):
        """Obtiene o crea un StorageHandler para un cliente."""
        if client_id not in self.storages_by_client:
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
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            
            if header and is_delete_packet(header):
                with self.lock:
                    self.output_rabbitmq.send_delete(client_id=client_id)
                    self.clean(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if is_final_packet(header):
                count = int(packet['count'])
                print(f" [*] Cola '{self.input_queue_1}' terminó.")
                with self.lock:
                    if client_id not in self.eof_main_by_client:
                        self.eof_main_by_client[client_id] = False
                    buffer_count = len(self.router_buffer_by_client.get(client_id, {}))
                    if self.eof_main_by_client[client_id] is True:
                        print("merge + final del main")
                        self.merge(client_id)
                        count_send = self.count_by_client[client_id]
                        self.final_rabbitmq.send_final_with_node_id(
                            client_id=client_id, node_id=self.node_id, count=count_send
                        )
                        self.clean(client_id)
                        print("merge + final del main fin")
                    else:
                        print("activo main")
                        self.eof_main_by_client[client_id] = True
                if count > buffer_count:
                    print(f" [⚠️] Count final ({count}) es MAYOR que los datos acumulados ({buffer_count}) para el cliente {client_id}")
                elif count < buffer_count:
                    print(f" [⚠️] Count final ({count}) es MENOR que los datos acumulados ({buffer_count}) para el cliente {client_id}")
                else:
                    print(f" [✅] Count final ({count}) COINCIDE con los datos acumulados ({buffer_count}) para el cliente {client_id}")
                   
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            router = int(movie.get(self.join_by))

            if not router:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            with self.lock:
                # Inicializar router_buffer para el cliente si no existe
                if client_id not in self.router_buffer_by_client:
                    self.router_buffer_by_client[client_id] = {}
                if router not in self.router_buffer_by_client[client_id]:
                    print(f" [🆕] Creating new router_buffer entry for router '{router}' para cliente '{client_id}'")
                    self.router_buffer_by_client[client_id][router] = movie
                    print(f" [✅] Router '{router}' entry saved para cliente '{client_id}'. Current buffer size: {len(self.router_buffer_by_client[client_id])}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def join_callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq_2.close_graceful(method)
                return
            
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            
            if header and is_delete_packet(header):
                with self.lock:
                    self.output_rabbitmq.send_delete(client_id=client_id)
                    self.clean(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if is_final_packet(header):
                print(f" [*] Cola '{self.input_queue_2}' terminó.")
                count = int(packet.get("count"))
                print(f" [✅] Count final ({count}) CONTRA con los datos acumulados ({self.count_test}) para el cliente {client_id}")
                with self.lock:
                    if self.eof_main_by_client[client_id] is True:
                        print("merge + final del join")
                        self.merge(client_id)
                        count_send = self.count_by_client[client_id]
                        self.final_rabbitmq.send_final_with_node_id(
                            client_id=client_id, node_id=self.node_id, count=count_send
                        )
                        self.clean(client_id) # Borro solo despues haber mandado el final (si crashea antes, pierdo el count) y antes del ACK (si crashea antes, se repite el clean)
                        print("merge + final del join fin")
                    else:
                        print("activo join")
                        self.eof_main_by_client[client_id] = True
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            router = int(movie.get(self.join_by))
            id = packet.id
            
            if not router:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            self.count_test += 1
            
            with self.lock:
                if client_id not in self.count_by_client:
                    self.count_by_client[client_id] = 0
                if client_id not in self.eof_main_by_client:
                    self.eof_main_by_client[client_id] = False
                router_in_buffer = router in self.router_buffer_by_client.get(client_id, {})
                is_eof_main = self.eof_main_by_client[client_id]
                
            if router_in_buffer:
                print(f" [🔍] Router '{router}' found in router_buffer")
                with self.lock:
                    movie1 = self.router_buffer_by_client[client_id][router]
                joined_packet = self.create_joined_packet(client_id, movie1, movie, id)
                with self.lock:
                    self.output_rabbitmq.publish(joined_packet.to_json())
                self.count_by_client[client_id] = self.count_by_client.get(client_id, 0) + 1
                print(f" [✓] Joined and published router '{router}' para cliente '{client_id}' to output_rabbitmq")
                
            else:
                # Si eof_main es False, guardar en el disco
                if not is_eof_main:
                    # Obtener el StorageHandler para el cliente
                    with self.lock:
                        storage = self._get_storage_for_client(client_id)
                        print(f" [💾] Router '{router}' not in buffer, adding to disk")
                        storage.add(str(router), movie, id)
                        print(f" [✅] Added router '{router}' to disk")  

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            print(f" [! spectacles for the error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def create_joined_packet(self, client_id: int, movie1, movie2, id):
        combined_movie = {**movie1, **movie2}
        
        joined_packet = DataPacket(
            client_id=client_id,
            timestamp=datetime.utcnow().isoformat(),
            data=combined_movie,
            keep_columns=self.keep_columns,
            id=str(id)
        )
        return joined_packet
    
    def merge(self, client_id):
        storage = self._get_storage_for_client(client_id)
        # Verificar si el disco está vacío
        stored_keys = storage.list_keys()
        if not stored_keys:
            return
        print(" [🔄] Iniciando merge completo (eof_main=True)")    
        # Realizar merge completo: combinar router_buffer con todos los datos del disco
        for key in stored_keys:
            router_key = int(key)  # Convertir la clave a entero
            # Proteger acceso a router_buffer_by_client
            if router_key in self.router_buffer_by_client.get(client_id, {}):
                movie1 = self.router_buffer_by_client[client_id][router_key]
            else:
                continue
            stored_movies = storage.retrieve(key)
            if stored_movies:
                # Asegurarse de que stored_movies sea una lista
                if not isinstance(stored_movies, list):
                    stored_movies = [stored_movies]
                print(f" [🔍] Procesando router '{router_key}' con {len(stored_movies)} entradas en disco")
                for movie2, id in stored_movies:
                    joined_packet = self.create_joined_packet(client_id, movie1, movie2, id)
                    self.output_rabbitmq.publish(joined_packet.to_json())
                    self.count_by_client[client_id] = self.count_by_client.get(client_id, 0) + 1
                    print(f" [✓] Joined and published router '{router_key}' from disk to output_rabbitmq")
            
        # Limpiar el disco después del merge
        storage.clean()
        print(f" [✅] Disco limpio")            
                
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
                  
        except Exception as e:
            print(f" [!] Error in join node: {e}")
        finally:
            if self.leader_queue:
                self.leader_queue.join()
            self.close()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.running = False
        if self.control:
            self.control.stop()
        if self.input_rabbitmq_1:
            self.input_rabbitmq_1.cancel_consumer()
        if self.input_rabbitmq_2:
            self.input_rabbitmq_2.cancel_consumer()
        if self.leader_queue:
            self.leader_queue.close()
    
    def clean(self, client_id):
        # Limpiar disco del cliente
        if client_id in self.storages_by_client:
            self.storages_by_client[client_id].clean()
            del self.storages_by_client[client_id]
            
        # Limpiar router_buffer del cliente
        if client_id in self.router_buffer_by_client:
            del self.router_buffer_by_client[client_id]
    
        # Limpiar eof_main del cliente
        if client_id in self.eof_main_by_client:
            del self.eof_main_by_client[client_id]
        # Limpiar count del cliente
        if client_id in self.count_by_client:
            del self.count_by_client[client_id]
        print(f" [✅] Disco limpio y memoria limpia para '{client_id}'") 

    def close(self):
        print(f"Closing queues")
        if self.leader_queue:
            self.leader_queue.close()
        if self.input_rabbitmq_1:
            self.input_rabbitmq_1.close()
        if self.input_rabbitmq_2:
            self.input_rabbitmq_2.close()
        if self.final_rabbitmq:
            self.final_rabbitmq.close()
        for client_id, storage in self.storages_by_client.items():
            print(f" [🧹] Limpiando almacenamiento para cliente '{client_id}'")
            storage.clean_all()
        self.storages_by_client.clear()
        self.router_buffer_by_client.clear()
