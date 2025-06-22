import os
import signal
import socket
import threading
from common.atomic_write import atomic_write
from common.middleware import Middleware
from common.worker_protocol import WorkerProtocol
from src.client_connection import ClientConnection
from common.middleware import Middleware
from src.leader_election import LeaderElector


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
            print(f"[Gateway] Error saving counter to {self.counter_file}: {self.e}")

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

        # Start only if I am the leader
        self.block_until_i_am_the_leader()

        self.server.listen(5)
        print(f"[Gateway] Escuchando en {self.host}:{self.port}...")

        try:
            while self.running:
                client_socket, addr = self.server.accept()
                print(f"[Gateway] Nueva conexión de {addr}")
                # Asignar un client_id único
                client_id = self.client_counter
                self.client_counter += 1
                self.save_counter()
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
        print(f"[Gateway ] Recibida señal SIGTERM")
        if self.control:
            self.control.stop()
        self.close()

    def close(self):
        """Cierra el servidor y todos los procesos."""
        self.running = False
        if self.server:
            self.server.close()
            print(f"[Gateway ] Servidor cerrado")

        if self.leader_elector:
            self.leader_elector.close()

        # Terminar todos los procesos
        for process in self.processes:
            process.finish()
        print(f"[Gateway ] Todos los procesos terminados")

    def start_leader_elector(self):
        """
        Starts the leader elector on a new thread

        The leader elector has a semaphore. When it becomes the leader,
        it will call release() on the semaphore.

        The main thread of the gateway is blocked waiting on semaphore.acquire()
        so it will unblock when the leader elector becomes the leader.
        """
        self.leader_elector_semaphore = threading.Semaphore(1)
        # Acquire the semaphore and hand it to the leader election participant
        self.leader_elector_semaphore.acquire()

        # Start the leader elector on a new thread.
        # If it becomes the leader, it will release the semaphore, allowing
        # the gateway to start
        self.leader_elector = LeaderElector(
            peer_id=self.node_id,
            number_of_peers=self.cluster_size,
            port=7777,
            peer_prefix="gateway",
            semaphore=self.leader_elector_semaphore,
        )

    def block_until_i_am_the_leader(self):
        """
        Blocks until the leader elector becomes the leader.
        """
        self.leader_elector_semaphore.acquire()
