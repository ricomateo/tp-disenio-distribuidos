import os
from src.protocol import Protocol, HEADER_MSG_TYPE, BATCH_MSG_TYPE
from common.middleware import Middleware


class Gateway:
    def __init__(self, host: str, port: int):
        output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")

        self.protocol = Protocol(host, port)
        self.header_by_file = dict()
        
        self.rabbitmq = Middleware(queue=output_queue)
        self.rabbitmq_receiver = Middleware(queue=input_queue)
    
    def start(self):
        while True:
            try:
                msg = self.protocol.recv_message()
                if msg["msg_type"] == HEADER_MSG_TYPE:
                    filename = msg["filename"]
                    header = msg["header"]
                    self.header_by_file[filename] = header
                    print(f"self.header_by_file = {self.header_by_file}")
                elif msg["msg_type"] == BATCH_MSG_TYPE:
                    msg_filename = msg["filename"]
                    msg_header = self.header_by_file[msg_filename]
                    msg["header"] = msg_header
                    print(f"batch message = {msg}")
                    self.publish_file_batch(msg)
            except ConnectionError:
                print(f"Client disconnected")
                break

    def publish_file_batch(self, batch: dict):
        self.rabbitmq.publish(batch)
        number_of_lines = len(batch.get("rows"))
        print(f"[✓] Publicadas {number_of_lines} líneas.")
