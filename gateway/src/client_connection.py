import os
import json
import signal
import socket
import multiprocessing
from src.protocol import Protocol
from common.protocol_constants import HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE, FIN_MSG_TYPE
from common.middleware import Middleware
from common.packet import is_final_packet

class ClientConnection:
    def __init__(self, socket, addr, client_id):
        """Inicializa el gateway para escuchar conexiones de clientes."""
        self.header_by_file = {}
        self.running = True
        self.client_id = client_id
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

    def handle_client(self, addr, client_id):
        """Maneja un cliente en un proceso separado."""
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        client_running = True

        try:
            while client_running:
                if self.running == False:
                    break
                
                msg = self.client.recv_message()
                if msg["msg_type"] == HEADER_MSG_TYPE:
                    filename = msg["filename"]
                    header = msg["header"]
                    self.header_by_file[filename] = header

                elif msg["msg_type"] == BATCH_MSG_TYPE:
                    msg_filename = msg["filename"]
                    msg_header = self.header_by_file[msg_filename]
                    msg["header"] = msg_header
                    msg["client_id"] = client_id 
                    self.publish_file_batch(msg, msg_filename)

                elif msg["msg_type"] == EOF_MSG_TYPE:
                    print(f"[Gateway - Client {client_id}] Archivo CSV recibido correctamente.")
                    self.rabbitmq.send_final(self.client_id, msg_filename)

                elif msg["msg_type"] == FIN_MSG_TYPE:
                    self._recv_results(addr, client_id)
                    client_running = False

        except ConnectionError:
            print(f"[Client {client_id}] Cliente desconectado")
        except Exception as e:
            print(f"[Client {client_id}] Error: {e}")
        finally:
            print(f"[Client {client_id}] Cerrando recursos del cliente")
            self.rabbitmq_receiver.delete_queue()
            self.close()

    def publish_file_batch(self, batch: dict, msg_filename):
        """Publica un batch de datos"""
        self.rabbitmq.publish(batch, msg_filename)

    def _recv_results(self, addr, client_id):
        """Recibe resultados de RabbitMQ y los envía al cliente."""

        def callback_reader(ch, method, properties, body):
            try:
                if self.running == False:
                    self.rabbitmq_receiver.close_graceful(method)
                    return

                packet_json = body.decode()
                packet = json.loads(packet_json)

                if is_final_packet(packet.get("header")):
                        self.client.send_finalization()
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        ch.stop_consuming()
                        return

                response_str = packet.get("response")
                if response_str:
                    print(f"[Gateway  - Client {client_id} - RESULT] Resultado final recibido:\n{response_str}")
                    self.client.send_result(response_str)
                else:
                    print(f"[Gateway - Client {client_id} - RESULT] Packet recibido sin campo 'response'. Ignorado.")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            except json.JSONDecodeError as e:
                print(f"[Client {client_id} - RESULT] Error decoding JSON: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
            except Exception as e:
                print(f"[Client {client_id} - RESULT] Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

        print(f"[Client {client_id}] Escuchando resultados en {self.input_queue}...")
        self.rabbitmq_receiver.consume(callback_reader)

    def _sigterm_handler(self, signum, _):
        """Maneja la señal SIGTERM para cerrar el servidor."""
        print(f"[Client {self.client_id}] Recibida señal SIGTERM")
        self.running = False
        self.rabbitmq_receiver.cancel_consumer()

    def close(self):
        """Cierra el servidor y todos los procesos."""
        try:
            self.rabbitmq.close()
            self.rabbitmq_receiver.close()
            self.client.close()
        except Exception as e:
            print(f"[Client {self.client_id}] Closing Error: {e}")
        
    def finish(self):
        if self.process.is_alive():
            self.process.terminate()
            self.process.join()
