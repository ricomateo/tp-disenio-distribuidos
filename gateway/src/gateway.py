import os
import json
import signal
from src.protocol import Protocol
from common.protocol_constants import HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE, FIN_MSG_TYPE
from common.middleware import Middleware
from common.packet import handle_final_packet, is_final_packet


class Gateway:
    def __init__(self, host: str, port: int):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")

        self.protocol = Protocol(host, port)
        self.header_by_file = dict()
        
        self.rabbitmq = Middleware(queue=output_queue)
        self.rabbitmq_receiver = Middleware(queue=input_queue)
    
    def start(self):
        try:
            while self.running:
                msg = self.protocol.recv_message()
                if msg["msg_type"] == HEADER_MSG_TYPE:
                    filename = msg["filename"]
                    header = msg["header"]
                    self.header_by_file[filename] = header

                elif msg["msg_type"] == BATCH_MSG_TYPE:
                    msg_filename = msg["filename"]
                    msg_header = self.header_by_file[msg_filename]
                    msg["header"] = msg_header
                    self.publish_file_batch(msg)

                elif msg["msg_type"] == EOF_MSG_TYPE:
                    print("[✓] Archivo CSV recibido correctamente.")
                    self.rabbitmq.publish(msg)

                elif msg["msg_type"] == FIN_MSG_TYPE:
                    self.rabbitmq_receiver.purge()
                    self.rabbitmq.send_final()
                    # TODO: agregar envio de resultados al cliente
                    self._recv_results()

        except ConnectionError:
            print(f"Client disconnected")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if self.running:
                self.close()

    def publish_file_batch(self, batch: dict):
        self.rabbitmq.publish(batch)
        number_of_lines = len(batch.get("rows"))
        print(f"[✓] Publicadas {number_of_lines} líneas.")

    def _recv_results(self):
        def callback_reader(ch, method, properties, body):
            try:
                packet_json = body.decode()
        
                if is_final_packet(json.loads(packet_json).get("header")):
                    if handle_final_packet(method, self.rabbitmq_receiver):
                        self.rabbitmq.send_final()
                        self.rabbitmq_receiver.send_ack_and_close(method)
                        self.send_finalization()
                    return
                
                response_str = json.loads(packet_json).get("response")
                if response_str:
                    print(" [GATEWAY - RESULT] Resultado final recibido:\n")
                    print(response_str)
                    self.send_result(response_str)
                else:
                    print(" [GATEWAY - RESULT] Packet recibido sin campo 'response'. Ignorado.")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            except json.JSONDecodeError as e:
                print(f" [GATEWAY - RESULT] Error decoding JSON: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
            except Exception as e:
                print(f" [GATEWAY - RESULT] Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)
        
        print(" [GATEWAY] Now listening for filtered movies in query_queue...")
        self.rabbitmq_receiver.consume(callback_reader)

    def send_result(self, result: str):
        self.protocol.send_result(result)

    def send_finalization(self):
        self.protocol.send_finalization()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()
    
    def close(self):
        print(f"Closing sockets")
        self.running = False
        self.protocol.close()
        self.rabbitmq.close()
        self.rabbitmq_receiver.close()
