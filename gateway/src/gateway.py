
import os
import signal
import socket
from common.middleware import Middleware
from common.worker_protocol import WorkerProtocol
from src.client_connection import ClientConnection
from common.middleware import Middleware

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
        self.server.listen(5)
        self.processes = []
        self.client_counter = 0  # Contador para asignar IDs a los clientes
        self.clients_dir = "clients"
        
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        
        if self.output_exchange:
            self.rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.rabbitmq = Middleware(queue=self.output_queue)
            
        self.control = WorkerProtocol(self.health_server_ip, self.health_server_port, self.health_server_port)
        
    def _cleanup_dead_clients(self):
        """Lee los archivos en el directorio de clientes y envía mensajes de eliminación."""
        os.makedirs(self.clients_dir, exist_ok=True)
        try:
            for filename in os.listdir(self.clients_dir):
                if filename.endswith('.txt'):
                    try:
                        client_id = int(filename[:-4])  # ej. 2.txt -> 2
                        file_path = os.path.join(self.clients_dir, filename)
                        print(f"[Gateway] Enviando mensaje de eliminación para cliente muerto {client_id}")

                        # Leer todas las routing keys (nombres de archivo) del archivo
                        with open(file_path, "r") as f:
                            routing_keys = [line.strip() for line in f if line.strip()]

                        # Enviar delete para cada routing_key asociada al cliente
                        for rk in routing_keys:
                            self.rabbitmq.send_delete(client_id=client_id, routing_key=rk)

                        # Eliminar el archivo luego de procesar
                        os.remove(file_path)

                    except ValueError:
                        print(f"[Gateway] Nombre de archivo inválido: {filename}")
                    except Exception as e:
                        print(f"[Gateway] Error al eliminar cliente {filename}: {e}")
        except Exception as e:
            print(f"[Gateway] Error al limpiar clientes muertos: {e}")
       

    def start(self):
        """Inicia el servidor y acepta conexiones de clientes."""
        print(f"[Gateway] Escuchando en {self.host}:{self.port}...")
        self._cleanup_dead_clients()
        try:
            while self.running:
                client_socket, addr = self.server.accept()
                print(f"[Gateway] Nueva conexión de {addr}")
                # Asignar un client_id único
                client_id = self.client_counter
                self.client_counter += 1
                # Crear un proceso para manejar el cliente
                process = ClientConnection(client_socket, addr, client_id, self.clients_dir)
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
        # Terminar todos los procesos
        for process in self.processes:
            process.finish()
        print(f"[Gateway ] Todos los procesos terminados")