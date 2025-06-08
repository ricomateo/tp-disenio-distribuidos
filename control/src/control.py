import glob
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import logging
import signal
import docker
from typing import Dict, Set

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class ControlNode:
    def __init__(self):
        self.node_name = int(os.getenv("NODE_NAME", "control1"))
        self.next_node = int(os.getenv("NEXT_NODE", "control2"))
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.worker_port = int(os.getenv("WORKER_PORT", "9000"))
        self.cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')
        self.sleep_interval = float(os.getenv("SLEEP_INTERVAL") or '0.07')
        self.restart_interval = float(os.getenv("RESTART_INTERVAL") or '5')
        included_containers_env = os.getenv("INCLUDED_CONTAINERS", "")
        self.included_containers = included_containers_env.split(',')
        only_healthcheck_str = os.getenv("ONLY_HEALTHCHECK", "0")
        self.only_healthcheck = only_healthcheck_str == "1"
        self.router = bool(os.getenv("ROUTER", ""))
        self.locks_por_cliente = {}
        self.locks_por_nodo = {}
        self.final_counts_por_cliente = {}
        self.dead_clients = set()
        self.restart_in_progress = {}
        self.worker_threads = {}
        
        self.state_dir = f"control_state_{self.node_name}"
        os.makedirs(self.state_dir, exist_ok=True)
        self.final_counts_file = os.path.join(self.state_dir, "final_counts.json")
        self.dead_clients_file = os.path.join(self.state_dir, "dead_clients.json")
        
        self.threads = []
        self.should_stop = threading.Event()
        self.save_lock = threading.Lock()
        self.load_state()
        self.docker_client = docker.from_env()
        # Registrar manejador de SIGTERM
        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def _sigterm_handler(self, signum, frame):
        logging.info("Recibida señal SIGTERM, deteniendo...")
        self.stop()
        
    def load_state(self):
        """Loads all persistent state from files."""
        logging.info("Loading system state...")
        
        # Load final counts
        if os.path.exists(self.final_counts_file):
            try:
                with open(self.final_counts_file, "r") as f:
                    self.final_counts_por_cliente = json.load(f)
                logging.info(f"Loaded final counts: {self.final_counts_por_cliente}")
            except Exception as e:
                logging.error(f"Error loading final counts state: {e}")
                self.final_counts_por_cliente = {}

        # Load dead clients
        if os.path.exists(self.dead_clients_file):
            try:
                with open(self.dead_clients_file, "r") as f:
                    # Convert list back to set
                    self.dead_clients = set(json.load(f)) 
                logging.info(f"Loaded dead clients: {self.dead_clients}")
            except Exception as e:
                logging.error(f"Error loading dead clients state: {e}")
                self.dead_clients = set()

        
        
    def save_ids_state(self, client_id: str, node_id: str, id: str, send: str):
        """
        Saves the unique IDs and their send values for a specific client and node.
        """
        ids_file = os.path.join(self.state_dir, f"{client_id}_{node_id}.json")
        ids_tmp_file = os.path.join(self.state_dir, f"{client_id}_{node_id}.tmp.json")
        lock_key = f"{client_id}_{node_id}"
        
        # Get or create lock for this node_id-client_id pair
        if lock_key not in self.locks_por_nodo:
            with threading.Lock():  # Ensure thread-safe creation of new lock
                if lock_key not in self.locks_por_nodo:
                    self.locks_por_nodo[lock_key] = threading.Lock()
        
        with self.locks_por_nodo[lock_key]:
            try:
                if os.path.exists(ids_file):
                    shutil.copy2(ids_file, ids_tmp_file)
                else:
                    open(ids_tmp_file, "a").close()

                # Append the new ID
                with open(ids_tmp_file, "a") as f:
                    json.dump({"id": id, "send": send}, f)
                    f.write("\n")
                
                # Atomically replace the original file
                os.replace(ids_tmp_file, ids_file)
                logging.debug(f"Appended ID {id} for client {client_id} on node {node_id}")
            except Exception as e:
                logging.error(f"Error saving IDs state for client {client_id} on node {node_id}: {e}")
                
    def delete_ids_state(self, client_id: str):
        """Deletes all ID state files for a specific client."""
        pattern = os.path.join(self.state_dir, f"{client_id}_*.json")
        client_files = glob.glob(pattern)
        relevant_locks = []
        for lock_key, lock in self.locks_por_nodo.items():
            if lock_key.startswith(f"{client_id}_"):
                relevant_locks.append(lock)
                lock.acquire()
        try:
            for file_path in client_files:
                try:
                    os.remove(file_path)
                    logging.debug(f"Deleted ID state file: {file_path}")
                except Exception as e:
                    logging.error(f"Error deleting file {file_path} for client {client_id}: {e}")

            if not client_files:
                logging.debug(f"No ID state files found for client {client_id}")
        except Exception as e:
            logging.error(f"Error listing files to delete for client {client_id}: {e}")
        finally:
            for lock in relevant_locks:
                lock.release()


                

    def save_final_counts_state(self):
        """
        Saves the entire final_counts_por_cliente dictionary.
        """
        tmp_file = self.final_counts_file + ".tmp"
        with self.save_lock:
            try:
                with open(tmp_file, "w") as f:
                    json.dump(self.final_counts_por_cliente, f)
                os.replace(tmp_file, self.final_counts_file)
                logging.debug("Saved final counts state.")
            except Exception as e:
                logging.error(f"Error saving final counts state: {e}")

    def save_dead_clients_state(self):
        """
        Saves the entire dead_clients set.
        Sets need to be converted to list for JSON serialization.
        """
        tmp_file = self.dead_clients_file + ".tmp"
        with self.save_lock:
            try:
                with open(tmp_file, "w") as f:
                    json.dump(list(self.dead_clients), f) # Convert set to list for JSON
                os.replace(tmp_file, self.dead_clients_file)
                logging.debug("Saved dead clients state.")
            except Exception as e:
                logging.error(f"Error saving dead clients state: {e}")
            
    def restart_node(self, nodo: str):
        try:
            container = self.docker_client.containers.get(nodo)
            logging.info(f"Reiniciando el contenedor: {nodo}...")
            container.restart()
            logging.info(f"Contenedor {nodo} reiniciado exitosamente.")
            time.sleep(self.restart_interval)
            if nodo in self.worker_threads:
                    self.restart_in_progress[nodo] = True
                    old_thread = self.worker_threads[nodo]
                    if old_thread.is_alive():
                        logging.info(f"Terminando hilo connect_to_worker para {nodo}")
                        old_thread.join()
                    self.restart_in_progress[nodo] = False
                    new_thread = threading.Thread(target=self.connect_to_worker, args=(nodo,), name=f"connect_to_worker_{nodo}")
                    self.worker_threads[nodo] = new_thread
                    self.threads.append(new_thread)
                    new_thread.start()
                    logging.info(f"Nuevo hilo connect_to_worker iniciado para {nodo}")
        except subprocess.CalledProcessError:
            logging.error(f"Error al reiniciar {nodo}")

    def read_until_newline(self, conn: socket.socket) -> str:
        """Lee datos desde el socket hasta encontrar '\n' o hasta que se cierre la conexión."""
        buffer = b""
        while not self.should_stop.is_set():
            try:
                data = conn.recv(1)  # Leer byte por byte
                if not data:  # Conexión cerrada
                    return ""
                buffer += data
                if data == b"\n":
                    return buffer.decode().strip()
            except socket.timeout:
                continue
            except socket.error:
                return ""
        return ""
    
    def _handle_op_insert_id(self, conn: socket.socket):
        """Handles operation code '1' (Insert ID)."""
        mensaje = self.read_until_newline(conn)
        if not mensaje:
            return # Let handle_id_client break the loop

        try:
           
            parts = mensaje.split("|", 3)  # Limit split to 3 parts
            if len(parts) != 4:
                raise ValueError("Invalid number of parts in message for insert ID")
            client_id, id_recibido, send_value, nodo = parts

            if client_id in self.dead_clients:
                conn.sendall(b"3\n")
                return

            client_finished = self.insert_id(client_id, id_recibido, send_value, nodo)

            response_prefix = b"1" if client_finished else b"0"
            
            if client_finished and self.router:
                    send_counts = self.count_send_values(client_id)
                    frequencies = b",".join(f"{k}:{v}".encode() for k, v in sorted(send_counts.items()))
                    response_message = response_prefix + b"|" + frequencies + b"\n"
            elif client_finished:
                    total_send = str(self.calculate_total_send(client_id)).encode()
                    response_message = response_prefix + b"|" + total_send + b"\n"
            else:
                    response_message = response_prefix + b"\n"
            
            conn.sendall(response_message)
            logging.info(f"Client {client_id} inserted ID {id_recibido}. Final signal: {client_finished if client_finished else 'None'}")

        except ValueError as ve:
            logging.error(f"Error parsing message for insert ID: {ve}. Message: '{mensaje}'")
            conn.sendall(b"ERROR| Invalid format. Expected client_id|id|send_value or client_id|id\n")
        except Exception as e:
            logging.error(f"Error processing ID insertion: {e}")
            conn.sendall(b"ERROR| Internal failure inserting ID\n")

    def _handle_op_receive_final_count(self, conn: socket.socket):
        """Handles operation code '4' (Receive Final Count)."""
        mensaje = self.read_until_newline(conn)
        if not mensaje:
            return # Let handle_id_client break the loop

        try:
            client_id, count_recibido_str = mensaje.split("|")
            count_recibido = int(count_recibido_str)

            if client_id in self.dead_clients:
                conn.sendall(b"3\n")
                return

            if client_id not in self.locks_por_cliente:
                self.locks_por_cliente[client_id] = threading.Lock()
            
            with self.locks_por_cliente[client_id]:
                is_match = self.receive_final_count(client_id, count_recibido) 
                
                response_prefix = b"1" if is_match else b"0"
            
                if is_match and self.router:
                    send_counts = self.count_send_values(client_id)
                    frequencies = b",".join(f"{k}:{v}".encode() for k, v in sorted(send_counts.items()))
                    response_message = response_prefix + b"|" + frequencies + b"\n"
                    
                elif is_match:
                    total_send = str(self.calculate_total_send(client_id)).encode()
                    response_message = response_prefix + b"|" + total_send + b"\n"
                else:
                    response_message = response_prefix + b"\n"
                
            conn.sendall(response_message)
            logging.info(f"Client {client_id} sent final count {count_recibido}. Match: {is_match}.") # Log total send only if match


        except ValueError as ve:
            logging.error(f"Error parsing message for final count: {ve}. Message: '{mensaje}'")
            conn.sendall(b"ERROR| Invalid format. Expected client_id|count\n")
        except Exception as e:
            logging.error(f"Error processing final count: {e}")
            conn.sendall(b"ERROR| Internal failure processing final count\n")
            
    
    def _handle_op_delete_client(self, conn: socket.socket):
        """Handles operation code '2' (Delete Client)."""
        client_id = self.read_until_newline(conn)
        if not client_id:
            return 

        if client_id not in self.locks_por_cliente:
            self.locks_por_cliente[client_id] = threading.Lock() 
        
        with self.locks_por_cliente[client_id]:
            self.delete_client(client_id)
        conn.sendall(b"OK\n")
        logging.info(f"Client {client_id} requested deletion.")
        
    def handle_id_client(self, conn: socket.socket, addr, nodo: str):
        logging.info(f"Nodo {nodo} conectado (ID/delete connection)")
        try:
            conn.settimeout(10)
            while not self.should_stop.is_set() and self.restart_in_progress[nodo] == False:
                try:
                    op_code = self.read_until_newline(conn)
                    if not op_code:
                        break 
                    
                    # Dispatch based on op_code
                    if op_code == "1":
                        self._handle_op_insert_id(conn)
                    elif op_code == "2": 
                        self._handle_op_delete_client(conn)
                    elif op_code == "3": 
                        self._handle_op_receive_final_count(conn)
                    else:
                        conn.sendall(b"ERROR| Invalid operation code\n")
                except socket.timeout:
                    continue
                except Exception as e:
                    logging.error(f"Error in handle_id_client for {nodo}: {e}")
                    break
        finally:
            conn.close()
            logging.info(f"Nodo {nodo} disconnected (ID/delete connection)")
            
    def _load_id_to_send(self, client_id: str) -> dict:
            """Loads and merges all id-to-send mappings for a specific client from all its node files."""
            id_to_send = {}

            # Encuentra todos los archivos que pertenecen a ese client_id
            pattern = os.path.join(self.state_dir, f"{client_id}_*.json")
            client_files = glob.glob(pattern)

            # Acquirir todos los locks relacionados a ese client_id
            relevant_locks = []
            for lock_key, lock in self.locks_por_nodo.items():
                if lock_key.startswith(f"{client_id}_"):
                    relevant_locks.append(lock)
                    lock.acquire()

            try:
                for file_path in client_files:
                    try:
                        with open(file_path, "r") as f:
                            for line in f:
                                try:
                                    data = json.loads(line.strip())
                                    id_value = data.get("id")
                                    send_value = data.get("send", "0")
                                    if id_value and id_value not in id_to_send:
                                        id_to_send[id_value] = int(send_value)
                                except json.JSONDecodeError:
                                    logging.warning(f"Invalid JSON line in {file_path}: {line.strip()}")
                                    continue
                    except Exception as e:
                        logging.error(f"Error reading file {file_path} for client {client_id}: {e}")
            finally:
                # Liberar todos los locks al final
                for lock in relevant_locks:
                    lock.release()

            if not client_files:
                logging.debug(f"No files found for client {client_id}")

            return id_to_send

    
    def calculate_total_send(self, client_id: str) -> int:
        """Calculates the total 'send' value for a given client dynamically from disk."""
        id_to_send = self._load_id_to_send(client_id)
        if not id_to_send:
            return 0
        return sum(id_to_send.values())

    def count_send_values(self, client_id: str) -> dict:
        """Counts the frequency of each send_value for a given client from disk."""
        id_to_send = self._load_id_to_send(client_id)
        if not id_to_send:  
            return {0: 0}
        send_counts = {}
        for send_value in id_to_send.values():
            send_counts[send_value] = send_counts.get(send_value, 0) + 1
        return send_counts

    def calculate_unique_id_count(self, client_id: str) -> int:
        """Calculates the count of unique IDs for a given client dynamically from disk."""
        id_to_send = self._load_id_to_send(client_id)
        return len(id_to_send)
            
    def insert_id(self, client_id: str, id_recibido: str, send: str, nodo: str) -> tuple[bool, int]:
        """
        Inserta un ID para un cliente dado, registrando el nodo de origen.
        Devuelve (True, count) si el ID ya existía (para *cualquier* nodo),
        o (False, count) si fue una inserción nueva.
        También actualiza el conteo de IDs únicos por cliente.
        """
        client_finished = None
        self.save_ids_state(client_id, nodo, id_recibido, send)
        logging.debug(f"ID {id_recibido} nuevo para cliente {client_id}. Envio: {send}")
        
        # Check if the total count matches the expected final count
        if client_id in self.final_counts_por_cliente:
            total_count = self.calculate_unique_id_count(client_id)
            if total_count >= self.final_counts_por_cliente[client_id]:
                client_finished = client_id
                logging.info(f"Client {client_id} current count {total_count} matches final count {self.final_counts_por_cliente[client_id]}.")

        return client_finished

    def delete_client(self, client_id: str):
        self.dead_clients.add(client_id)
        self.save_dead_clients_state()
        if client_id in self.final_counts_por_cliente:
            del self.final_counts_por_cliente[client_id]
        self.save_final_counts_state()
        self.delete_ids_state(client_id)
            
                
    def receive_final_count(self, client_id: str, count_recibido: int) -> bool:
        """
        Recibe el conteo final de un cliente y lo guarda.
        Devuelve True si el conteo recibido coincide con el conteo actual de IDs únicos, False en caso contrario.
        Assumes lock for client_id is held by the caller.
        """
        self.final_counts_por_cliente[client_id] = count_recibido
        self.save_final_counts_state()
        current_unique_id_count = self.calculate_unique_id_count(client_id)
        is_match = (current_unique_id_count >= count_recibido)
        
        logging.info(f"Received final count for client {client_id}: {count_recibido}. Current unique IDs: {current_unique_id_count}. Match: {is_match}")
        
        return is_match
    
    def handle_health_worker(self, nodo: str):
        while not self.should_stop.is_set():
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.settimeout(self.restart_interval)
                client_socket.connect((nodo, self.health_server_port))
                logging.info(f"Nodo {nodo} conectado (Healthcheck connection)")
                client_socket.close()
                time.sleep(self.sleep_interval)
            except Exception as e:
                logging.warning(f"Nodo {nodo} no respondió al healthcheck: {e}, intentando reiniciar...")
                self.restart_node(nodo)

    def healthcheck_next_control(self):
        if (self.next_node == 0):
            host = f'control'
        else:
            host = f'control_{self.next_node}'
        
        while not self.should_stop.is_set():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.restart_interval)
                sock.connect((host, self.health_server_port))
                logging.info(f"Nodo {host} conectado (Healthcheck connection)")
                sock.close()
                time.sleep(self.sleep_interval)
            except Exception as e:
                logging.warning(f"Nodo de control {host} no responde: {e}, intentando reiniciar...")
                self.restart_node(host)
            

    def control_health_server(self):
        health_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        health_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        health_server.bind((self.health_server_ip, self.health_server_port))
        health_server.listen()
        while not self.should_stop.is_set():
            try:
                health_server.settimeout(self.restart_interval)
                conn, addr = health_server.accept()
                conn.close()  # Solo aceptar y cerrar
            except socket.timeout:
                continue
            except Exception as e:
                logging.error(f"Error en control_health_server: {e}")
        health_server.close()

    def connect_to_worker(self, nodo: str):
        while not self.should_stop.is_set() and self.restart_in_progress[nodo] == False:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.restart_interval)
                sock.connect((nodo, self.worker_port)) 
                self.handle_id_client(sock, sock.getpeername(), nodo)
                time.sleep(self.restart_interval)
            except Exception as e:
                logging.warning(f"Fallo conexión inicial con {nodo}: {e}")
                time.sleep(self.restart_interval)

    def run(self):
        # Iniciar threads y almacenarlos
      
        t = threading.Thread(target=self.control_health_server, name="control_health_server")
        t.start()
        self.threads.append(t)
        
        time.sleep(self.restart_interval)
        
        threads = [
            threading.Thread(target=self.healthcheck_next_control, name="healthcheck_next_control")
        ]
        
        for nodo in self.included_containers:
            if not self.only_healthcheck:
             self.restart_in_progress[nodo] = False
             worker_thread = threading.Thread(target=self.connect_to_worker, args=(nodo,), name=f"connect_to_worker_{nodo}")
             self.worker_threads[nodo] = worker_thread
             threads.append(worker_thread)
            threads.append(threading.Thread(target=self.handle_health_worker, args=(nodo,), name=f"health_worker_{nodo}"))

        for t in threads:
            t.start()
            self.threads.append(t)
            
        threads[0].join()

    def stop(self):
        """Detener todos los threads y cerrar sockets."""
        logging.info("Deteniendo ControlNode...")
        self.should_stop.set()
        for t in self.threads:
            t.join()
        self.threads.clear()
        logging.info("ControlNode detenido")

