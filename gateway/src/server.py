import os
import json
from src.protocol import Protocol, HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE
from common.middleware import Middleware
from common.packet import handle_final_packet, is_final_packet


class Gateway:
    def __init__(self, host: str, port: int):
        output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")

        self.protocol = Protocol(host, port)
        self.header_by_file = dict()
        
        self.rabbitmq = Middleware(queue=output_queue)
        self.rabbitmq_receiver = Middleware(queue=input_queue)
    
    def start(self):
        count_batches = 0
        while True:
            try:
                msg = self.protocol.recv_message()
                if msg["msg_type"] == HEADER_MSG_TYPE:
                    filename = msg["filename"]
                    header = msg["header"]
                    self.header_by_file[filename] = header
                    print(f"self.header_by_file = {self.header_by_file}")
                elif msg["msg_type"] == BATCH_MSG_TYPE:
                    count_batches += 1
                    msg_filename = msg["filename"]
                    msg_header = self.header_by_file[msg_filename]
                    msg["header"] = msg_header
                    print(f"batch message = {msg}")
                    self.publish_file_batch(msg)
                    print(f"count_batches = {count_batches}")
                elif msg["msg_type"] == EOF_MSG_TYPE:
                    print("[✓] Archivo CSV recibido correctamente.")
                    # Publicar el FINAL PACKET
                    # self.rabbitmq_receiver.purge()
                    # self.rabbitmq.send_final()
                    # self.rabbitmq.close()
                    # self.rabbitmq_receiver.close()
                    # # TODO: mejorar esto
                    # def callback_reader(ch, method, properties, body):
                    #     try:
                    #         packet_json = body.decode()
                    
                    #         if is_final_packet(json.loads(packet_json).get("header")):
                    #             if handle_final_packet(method, self.rabbitmq_receiver):
                    #                 self.rabbitmq.send_final()
                    #                 self.rabbitmq_receiver.send_ack_and_close(method)
                    #             return
                            
                    #         response_str = json.loads(packet_json).get("response")
                    #         if response_str:
                    #             print(" [GATEWAY - RESULT] Resultado final recibido:\n")
                    #             print(response_str)
                    #         else:
                    #             print(" [GATEWAY - RESULT] Packet recibido sin campo 'response'. Ignorado.")

                    #         ch.basic_ack(delivery_tag=method.delivery_tag)
                    #     except json.JSONDecodeError as e:
                    #         print(f" [GATEWAY - RESULT] Error decoding JSON: {e}")
                    #         ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
                    #     except Exception as e:
                    #         print(f" [GATEWAY - RESULT] Error processing message: {e}")
                    #         ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)
                    # self.rabbitmq_receiver.consume(callback_reader)
                    # return

            except ConnectionError:
                print(f"Client disconnected")
                # TODO: mejorar el cierre
                self.rabbitmq.close()
                self.rabbitmq_receiver.close()
                return
            

    def publish_file_batch(self, batch: dict):
        self.rabbitmq.publish(batch)
        number_of_lines = len(batch.get("rows"))
        print(f"[✓] Publicadas {number_of_lines} líneas.")
