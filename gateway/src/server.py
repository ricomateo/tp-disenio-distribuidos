import os
from protocol import Protocol, HEADER_MSG_TYPE, BATCH_MSG_TYPE
# from common.middleware import Middleware


class Gateway:
    def __init__(self, host: str, port: int):
        host = host or os.getenv('GATEWAY_HOST', '0.0.0.0')
        port = port or int(os.getenv('GATEWAY_PORT', '9999'))
        output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
        input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue")

        self.protocol = Protocol(host, port)
        self.header_by_file = dict()
        
        # self.rabbitmq = Middleware(queue=output_queue)
        # self.rabbitmq_receiver = Middleware(queue=input_queue)
    
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
            except ConnectionError:
                print(f"Client disconnected")

def main():
    gateway = Gateway("0.0.0.0", 9999)
    gateway.start()
    # TODO: agregar cierre

main()
