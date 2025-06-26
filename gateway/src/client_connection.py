import os
import json
import signal
import multiprocessing
import logging
from common.logger import init_logging
from common.atomic_write import atomic_write
from src.protocol import Protocol
from common.protocol_constants import HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE, FIN_MSG_TYPE
from common.middleware import Middleware
from common.packet import is_final_packet

init_logging(os.getenv("LOG_LEVEL", "info"))

class ClientConnection:
    def __init__(self, socket, addr, client_id, clients_dir, ):
        """Inicializa el gateway para escuchar conexiones de clientes."""
        self.header_by_file = {}
        self.batch_count_by_file = {}
        self.running = True
        self.client_id = client_id
        self.clients_dir = clients_dir
        self.client = Protocol(socket)
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")

        if self.output_exchange:
            self.rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.rabbitmq = Middleware(queue=self.output_queue)
        
        self.rabbitmq_receiver = Middleware(
                queue=str(self.client_id),
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False,
                routing_key=str(self.client_id)
        )
        
        self.process = multiprocessing.Process(
                    target=self.handle_client,
                    args=(addr, client_id)
        )
        self.process.start()
        
    def _save_client_filenames(self, filenames):
        """Guarda los nombres de archivo (routing_keys) en <client_id>.txt."""
        try:
            os.makedirs(self.clients_dir, exist_ok=True)
            client_file = os.path.join(self.clients_dir, f"{self.client_id}.txt")
            content = "\n".join(filenames) + "\n"
            atomic_write(client_file, content)
        except Exception as e:
            logging.error("[ClientConnection] Error al guardar archivos del cliente %s: %s", self.client_id, e)

    def _remove_client(self):
        """Elimina el archivo <client_id>.txt del directorio."""
        try:
            client_file = os.path.join(self.clients_dir, f"{self.client_id}.txt")
            if os.path.exists(client_file):
                os.remove(client_file)
        except Exception as e:
            logging.warning("[ClientConnection] Error al eliminar archivo %s:%s", self.client_id, e)

    def handle_client(self, addr, client_id):
        """Maneja un cliente en un proceso separado."""
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        client_running = True

        try:
            while client_running:
                if not self.running:
                    break
                
                msg = self.client.recv_message()
                if msg["msg_type"] == HEADER_MSG_TYPE:
                    filename = msg["filename"]
                    header = msg["header"]
                    self.header_by_file[filename] = header
                    self.batch_count_by_file[filename] = 0
                    self._save_client_filenames(self.batch_count_by_file.keys())

                elif msg["msg_type"] == BATCH_MSG_TYPE:
                    msg_filename = msg["filename"]
                    msg_header = self.header_by_file[msg_filename]
                    msg["header"] = msg_header
                    msg["client_id"] = client_id
                    msg["id"] = self.batch_count_by_file[filename]
                    self.batch_count_by_file[filename] += 1
                    self.publish_file_batch(msg, msg_filename)

                elif msg["msg_type"] == EOF_MSG_TYPE:
                    logging.debug("[Gateway - Client %s] Archivo CSV recibido correctamente.", client_id)
                    msg_filename = msg["filename"]
                    self.rabbitmq.send_final(self.client_id, msg_filename, self.batch_count_by_file[filename])

                elif msg["msg_type"] == FIN_MSG_TYPE:
                    self._recv_results(addr, client_id)
                    client_running = False

        except ConnectionError:
            logging.warning("[Client %s] Cliente desconectado", client_id)
            self.send_delete()
        except Exception as e:
            logging.warning("[Client %s] Error: %s", client_id, e)
            self.send_delete()
        finally:
            logging.info("[Client %s] Cerrando recursos del cliente", client_id)
            self.close()

    def publish_file_batch(self, batch: dict, msg_filename):
        """Publica un batch de datos"""
        self.rabbitmq.publish(batch, msg_filename)

    def _recv_results(self, addr, client_id):
        """Recibe resultados de RabbitMQ y los envía al cliente."""

        def callback_reader(ch, method, properties, body):
            try:
                if self.running is False:
                    self.rabbitmq_receiver.close_graceful(method)
                    self.rabbitmq_receiver.delete_queue(str(self.client_id))
                    return

                packet_json = body.decode()
                packet = json.loads(packet_json)

                if is_final_packet(packet.get("header")):
                    self.client.send_finalization()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    ch.stop_consuming()
                    self._remove_client()
                    self.rabbitmq_receiver.delete_queue(str(self.client_id))
                    return

                response_str = packet.get("response")
                if response_str:
                    logging.debug("[Gateway  - Client %s - RESULT] Resultado final recibido:\n%s", client_id, response_str)
                    if not self.client.send_result(response_str):
                       ch.basic_ack(delivery_tag=method.delivery_tag)
                       ch.stop_consuming() 
                       self.send_delete()
                       self.rabbitmq_receiver.delete_queue(str(self.client_id))
                       return
                else:
                    logging.warning(f"[Gateway - Client %s - RESULT] Packet recibido sin campo 'response'. Ignorado.", client_id)

                ch.basic_ack(delivery_tag=method.delivery_tag)
            except json.JSONDecodeError as e:
                logging.warning("[Client %s - RESULT] Error decoding JSON: %s", client_id, e)
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
            except Exception as e:
                logging.warning("[Client %s - RESULT] Error processing message: %s", client_id, e)
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

        logging.info("[Client %s] Escuchando resultados en %s...", client_id, self.input_queue)
        self.rabbitmq_receiver.consume(callback_reader)

    def _sigterm_handler(self, signum, _):
        """Maneja la señal SIGTERM para cerrar el servidor."""
        logging.info("[Client %s] Recibida señal SIGTERM", self.client_id)
        self.running = False
        if self.rabbitmq_receiver:
            self.rabbitmq_receiver.cancel_consumer()
        self.client.close()

    def close(self):
        """Cierra el servidor y todos los procesos."""
        try:
            self.rabbitmq.close()
            self.rabbitmq_receiver.close()
            self.client.close()
        except Exception as e:
            logging.info("[Client %s] Closing Error: {e}", self.client_id)

    def finish(self):
        if self.process.is_alive():
            self.process.terminate()
            self.process.join()

    def send_delete(self):
        for fname in self.batch_count_by_file:
            self.rabbitmq.send_delete(client_id=self.client_id, routing_key=fname)
        self._remove_client()
