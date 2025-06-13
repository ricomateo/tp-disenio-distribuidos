import socket
import sys
import threading
import time
import logging

class WorkerProtocol:
    def __init__(self, host: str, port: int, health_port: int):
        """
        Inicializa el protocolo del worker para comunicarse con el nodo de control.
        
        """
        self.host = host
        self.port = port
        self.health_port = health_port
        self.conn = None
        self.is_running = True  
        self.healthcheck = threading.Thread(target=self.healthcheck_listen, name="control_health_server")
        self.healthcheck.start()

    def listen(self):
        health_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        health_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        health_server.bind((self.host, self.port))
        health_server.listen()
        while self.is_running:
            try:
                health_server.settimeout(3) 
                conn, addr = health_server.accept()
                self.conn = conn
                break
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Error en control_health_server: {e}")
        health_server.close()
        
    def healthcheck_listen(self):
        health_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        health_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        health_server.bind((self.host, self.health_port))
        health_server.listen()
        while self.is_running:
            try:
                health_server.settimeout(3)  # Timeout para verificar should_stop
                conn, addr = health_server.accept()
                conn.close()  # Solo aceptar y cerrar
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Error en control_health_server: {e}")
        health_server.close()

    def read_until_newline(self) -> str:
        """
        Lee datos desde el socket hasta encontrar '\n' o hasta que se cierre la conexión.
        Devuelve una cadena vacía en caso de error o timeout.
        """
        if not self.conn:
            return ""
        buffer = b""
        try:
            while self.is_running:
                data = self.conn.recv(1)
                if not data:
                    return ""
                buffer += data
                if data == b"\n":
                    return buffer.decode().strip()
        except socket.timeout:
            logging.warning(f"Timeout al leer desde {self.host}")
            return ""
        except socket.error as e:
            logging.error(f"Error al leer desde {self.host}: {e}")
            return ""
        return ""

    def send_message(self, message: str) -> bool:
        """
        Envía un mensaje al controlador y asegura que termine con '\n'.
        
        Args:
            message (str): Mensaje a enviar (sin '\n').
        Returns:
            bool: True si el envío fue exitoso, False si falló.
        """
        if not self.conn:
            logging.error(f"No hay conexión activa con {self.host}")
            return False
        try:
            self.conn.sendall(f"{message}\n".encode())
            return True
        except Exception as e:
            logging.error(f"Error al enviar mensaje a {self.host}: {e}")
            self.conn = None
            return False

    def insert_id(self, client_id: str, id_recibido: str, send_value: str, node: str) -> tuple[bool, str]:
        """
        Envía una operación de inserción de ID (op_code=1) al controlador.
        
        Args:
            client_id (str): ID del cliente.
            id_recibido (str): ID a insertar.
            send_value (str): Valor de envío asociado.
        Returns:
            tuple[bool, str]: (Éxito, Respuesta del controlador).
        """
        message = f"1\n{client_id}|{id_recibido}|{send_value}|{node}"
        
        sended = self.send_message(message)
        if not sended and self.is_running:
            self.listen()
            return self.insert_id(client_id, id_recibido, send_value, node)
        elif not sended:
            return False, ""
        
        response = self.read_until_newline()
        if not response and self.is_running:
            self.listen()
            return self.insert_id(client_id, id_recibido, send_value, node)
        elif not response:
            return False, ""
        
        parts = response.split("|")
        if parts[0] == "ERROR":
            logging.error(f"Error del controlador: {response}")
            return False, response
      
        success = parts[0] == "1"
        return success, parts[1] if len(parts) > 1 else ""

    def delete_client(self, client_id: str) -> bool:
        """
        Envía una operación de eliminación de cliente (op_code=2) al controlador.
        
        Args:
            client_id (str): ID del cliente a eliminar.
        Returns:
            bool: True si la operación fue exitosa, False si falló.
        """
        message = f"2\n{client_id}"
        
        sended = self.send_message(message)
        if not sended and self.is_running:
            self.listen()
            return self.delete_client(client_id)
        elif not sended:
            return False
        
        response = self.read_until_newline()
        if not response and self.is_running:
            self.listen()
            return self.delete_client(client_id)
        elif not response:
            return False
        elif response == "OK":
            return True
        logging.error(f"Error del controlador al eliminar cliente: {response}")
        return False

    def send_final_count(self, client_id: str, count: int) -> tuple[bool, str]:
        """
        Envía el conteo final (op_code=3) al controlador.
        
        Args:
            client_id (str): ID del cliente.
            count (int): Conteo final a enviar.
        Returns:
            tuple[bool, str]: (Éxito, Respuesta del controlador).
        """
        message = f"3\n{client_id}|{count}"
        sended = self.send_message(message)
        if not sended and self.is_running:
            self.listen()
            return self.send_final_count(client_id, count)
        elif not sended:
            return False, ""
        
        response = self.read_until_newline()
        if not response and self.is_running:
            self.listen()
            return self.send_final_count(client_id, count)
        elif not response:
            return False, ""
        
        parts = response.split("|")
        if parts[0] == "ERROR":
            logging.error(f"Error del controlador: {response}")
            return False, response

        success = parts[0] == "1"
        return success, parts[1] if len(parts) > 1 else ""

    def stop(self):
        """Detiene el protocolo y cierra la conexión."""
        self.is_running = False
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None 
        self.healthcheck.join()
