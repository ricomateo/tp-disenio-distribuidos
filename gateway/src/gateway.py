import os
import signal
import socket
import threading
import time
from common.atomic_write import atomic_write
from common.middleware import Middleware
from common.worker_protocol import WorkerProtocol
from src.client_connection import ClientConnection
from common.middleware import Middleware
from src.leader_election import LeaderElector
from src.gateway_connection import GatewayConnection


class Gateway:
    def __init__(self, host: str, port: int):
        """Inicializa el gateway para escuchar conexiones de clientes."""
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.host = host
        self.port = port
        self.running = True
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.processes = []
        # Contador para asignar IDs a los clientes
        self.clients_dir = "clients"
        self.counter_file = "clients_counter.txt"
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        self.node_id = int(os.getenv("NODE_ID"))
        self.cluster_size = int(os.getenv("CLUSTER_SIZE"))

        self.gateway_connection = GatewayConnection()
        self.replicas_listener = None
        self.client_count_listener_thread = None

        if self.output_exchange:
            self.rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.rabbitmq = Middleware(queue=self.output_queue)

        self.control = WorkerProtocol(
            self.health_server_ip, self.health_server_port, self.health_server_port
        )

        self.client_counter = self.load_counter()
        self.start_leader_elector()

    def _cleanup_dead_clients(self):
        """Lee los archivos en el directorio de clientes y envía mensajes de eliminación."""
        os.makedirs(self.clients_dir, exist_ok=True)
        try:
            for filename in os.listdir(self.clients_dir):
                if filename.endswith(".txt"):
                    try:
                        client_id = int(filename[:-4])  # ej. 2.txt -> 2
                        file_path = os.path.join(self.clients_dir, filename)
                        print(
                            f"[Gateway] Enviando mensaje de eliminación para cliente muerto {client_id}"
                        )

                        # Leer todas las routing keys (nombres de archivo) del archivo
                        with open(file_path, "r") as f:
                            routing_keys = [line.strip() for line in f if line.strip()]

                        # Enviar delete para cada routing_key asociada al cliente
                        for rk in routing_keys:
                            self.rabbitmq.send_delete(
                                client_id=client_id, routing_key=rk
                            )

                        # Eliminar el archivo luego de procesar
                        os.remove(file_path)

                    except ValueError:
                        print(f"[Gateway] Nombre de archivo inválido: {filename}")
                    except Exception as e:
                        print(f"[Gateway] Error al eliminar cliente {filename}: {e}")
        except Exception as e:
            print(f"[Gateway] Error al limpiar clientes muertos: {e}")

    def save_counter(self):
        """Saves the client counter to disk."""
        try:
            atomic_write(self.counter_file, str(self.client_counter))
        except Exception as e:
            print(f"[Gateway] Error saving counter to {self.counter_file}: {e}")

    def load_counter(self):
        """Loads the client counter from disk."""
        try:
            with open(self.counter_file, "r") as f:
                content = f.read().strip()
                if not content:  # Handle empty file
                    print(
                        f"[Gateway] Empty counter file at {self.counter_file}, starting with 0"
                    )
                    return 0
                counter = int(content)  # Parse string to integer
                if counter < 0:
                    raise ValueError("Counter cannot be negative")
                return counter
        except FileNotFoundError:
            print(
                f"[Gateway] No counter file found at {self.counter_file}, starting with 0"
            )
            return 0
        except (ValueError, OSError) as e:
            print(
                f"[Gateway] Error reading counter from {self.counter_file}: {e}, starting with 0"
            )
            return 0

    def start(self):
        """Inicia el servidor y acepta conexiones de clientes."""
        self._cleanup_dead_clients()

        # Block until a leader is elected
        self.block_until_a_leader_is_elected()

        # The semaphore may be released by the close() function
        # so we check if we are still running
        if not self.running:
            return

        if not self.am_i_leader():
            # If i am not the leader, start a thread that connects
            # to the leader
            self.client_count_listener_thread = threading.Thread(
                target=self.listen_for_client_count
            )
            self.client_count_listener_thread.start()
            
            # Loop until I become the leader or a SIGTERM is received
            while True:
                self.block_until_a_leader_is_elected()
                if not self.running:
                    return
                if self.am_i_leader():
                    # Join the listener thread and create a new GatewayConnection
                    # This is required to use a clean socket
                    self.gateway_connection.close()
                    self.client_count_listener_thread.join()
                    self.client_count_listener_thread = None
                    self.gateway_connection = GatewayConnection()
                    break

        # As the leader, start a thread listening for
        # messages from the gateway replicas
        self.replicas_listener = threading.Thread(
            target=self.listen_for_replicas_messages
        )
        self.replicas_listener.start()
        try:
            self.server.listen(5)
            print(f"[Gateway] Escuchando en {self.host}:{self.port}...")
            while self.running:
                client_socket, addr = self.server.accept()
                print(f"[Gateway] Nueva conexión de {addr}")
                # Asignar un client_id único
                client_id = self.client_counter
                self.client_counter += 1
                self.save_counter()
                self.broadcast_client_count()
                # Crear un proceso para manejar el cliente
                process = ClientConnection(
                    client_socket, addr, client_id, self.clients_dir
                )
                self.processes.append(process)

        except Exception as e:
            print(f"[Gateway] Error en el servidor: {e}")
        finally:
            if self.running == True:
                self.close()

    def _sigterm_handler(self, signum, _):
        """Maneja la señal SIGTERM para cerrar el servidor."""
        print("[Gateway ] Recibida señal SIGTERM")
        if self.control:
            self.control.stop()
        self.close()

    def close(self):
        """Cierra el servidor y todos los procesos."""
        self.running = False
        if self.server:
            self.server.close()
            print("[Gateway ] Servidor cerrado")


        if self.replicas_listener:
            self.gateway_connection.close()
            self.replicas_listener.join()
            print("[Gateway] Replicas listener cerrado")

        if self.client_count_listener_thread:
            self.gateway_connection.close()
            self.client_count_listener_thread.join()
            print("[Gateway] Client count listener cerrado")

        self.leader_elector_semaphore.release()
        if self.leader_elector:
            self.leader_elector.close()
            print("[Gateway] Leader elector cerrado")

        # Terminar todos los procesos
        for process in self.processes:
            process.finish()
        print("[Gateway ] Todos los procesos terminados")
        

    def start_leader_elector(self):
        """
        Starts the leader elector on a new thread

        The leader elector has a semaphore that signals when a
        new leader is elected.

        The main thread of the gateway is blocked waiting on semaphore.acquire()
        so it will unblock when a new leader is elected.
        """
        # Acquire the semaphore and hand it to the leader election participant
        self.leader_elector_semaphore = threading.Semaphore(1)
        self.leader_elector_semaphore.acquire()

        # Start the leader elector on a new thread.
        # When a leader is elected, the leader elector will call
        # release() on the semaphore, unblocking the gateway
        self.leader_elector = LeaderElector(
            peer_id=self.node_id,
            number_of_peers=self.cluster_size,
            port=7777,
            peer_prefix="gateway",
            semaphore=self.leader_elector_semaphore,
        )

    def block_until_a_leader_is_elected(self):
        self.leader_elector_semaphore.acquire()

    def listen_for_client_count(self):
        """
        Requests the client count to the leader, and then just
        waits for incoming messages from the leader.
        This should only be executed by the Gateway replicas (not the leader)
        """
        listening = True
        leader_id = self.get_leader_id()
        
        if leader_id is not None:
            # Request the first client count to the leader
            leader_address = f"gateway_{leader_id}"
            if leader_id == 0:
                leader_address = "gateway"
            time.sleep(0.1)
            # Try 5 times
            for _ in range(5):
                try:
                    self.gateway_connection.send_client_count_request(leader_address)
                    print(f"Sent client_count_request to the leader {leader_address}!")
                    break
                except Exception as e:
                    print(f"Failed to request client_count_request. Error: {e}")
                    # Backoff
                    time.sleep(0.1)
                    continue

        while listening:
            try:
                self.client_counter = self.gateway_connection.recv_client_count()
                print(f"New client count = {self.client_counter}")
                self.save_counter()
            except OSError:
                listening = False
            except Exception as e:
                print(f"Failed to receive client count. Error: {e}")

    def listen_for_replicas_messages(self):
        """
        Listens for messages from the replicas (requests for the client count)
        This should only be executed by the Gateway leader (not the replicas)
        """
        listening = True
        print("Waiting for incoming replica messages")
        while listening:
            try:
                message = self.gateway_connection.recv_replica_message()
                if message.get("msg_type") == "client_count_request":
                    replica_address, _ = message.get("from")
                    self.gateway_connection.send_client_count(replica_address, self.client_counter)
                    print("Received client_count_request message")
                else:
                    print(f"Received unknown message {message}")
            except OSError:
                print("Disconnecting...")
                listening = False
            except Exception as e:
                print(f"Failed to receive client count. Error: {e}")

    def broadcast_client_count(self):
        """
        Broadcasts the client count to all the Gateway replicas.
        This should only be executed by the Gateway leader.
        """
        addresses = []
        for i in range(self.cluster_size):
            if i == self.node_id:
                continue
            address = f"gateway_{i}"
            if i == 0:
                address = "gateway"
            addresses.append(address)
        for address in addresses:
            try:
                self.gateway_connection.send_client_count(address, self.client_counter)
            except Exception as e:
                print(f"Failed to send client count to address {address}. Error: {e}")


    def am_i_leader(self) -> bool:
        leader_id = self.get_leader_id()
        return self.node_id == leader_id

    def get_leader_id(self) -> int:
        with self.leader_elector.current_leader_lock:
            return self.leader_elector.current_leader
