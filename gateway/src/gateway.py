
import signal
import socket
from src.client_connection import ClientConnection


class Gateway:
    def __init__(self, host: str, port: int):
        """Inicializa el gateway para escuchar conexiones de clientes."""
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.host = host
        self.port = port
        self.running = True
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(5)
        self.processes = []
        self.client_counter = 0  # Contador para asignar IDs a los clientes
      

    def start(self):
        """Inicia el servidor y acepta conexiones de clientes."""
        print(f"[Gateway] Escuchando en {self.host}:{self.port}...")
        try:
            while self.running:
                client_socket, addr = self.server.accept()
                print(f"[Gateway] Nueva conexión de {addr}")
                # Asignar un client_id único
                client_id = self.client_counter
                self.client_counter += 1
                # Crear un proceso para manejar el cliente
                process = ClientConnection(client_socket, addr, client_id)
                self.processes.append(process)
           
        except Exception as e:
            print(f"[Gateway] Error en el servidor: {e}")
        finally:
            if self.running == True:
                self.close()

    def _sigterm_handler(self, signum, _):
        """Maneja la señal SIGTERM para cerrar el servidor."""
        print(f"[Gateway ] Recibida señal SIGTERM")
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