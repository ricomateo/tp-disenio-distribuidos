# gateway.py
import socket
# import json
import os
from protocol import Protocol, HEADER_MSG_TYPE


class Gateway:
    def __init__(self, host: str, port: int):
        self.protocol = Protocol(host, port)
        self.header_by_file = {}
    
    def start(self):
        while True:
            msg = self.protocol.recv_message()
            if msg["msg_type"] == HEADER_MSG_TYPE:
                filename = msg["filename"]
                header = msg["header"]
                self.header_by_file[filename] = header
                print(f"self.header_by_file = {self.header_by_file}")


def start_gateway(host = None, port = None):
    # Configurar middleware de RabbitMQ
    output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
    input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue") 
    # rabbitmq = Middleware(queue=output_queue)
    # rabbitmq_receiver = Middleware(queue=input_queue)
    host = host or os.getenv('GATEWAY_HOST', '0.0.0.0') # Escucha en todas las interfaces
    port = port or int(os.getenv('GATEWAY_PORT', '9999'))
    batch_size = int(os.getenv('BATCH_SIZE', '100'))
    
    # Configurar socket TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f" [x] Gateway listening on {host}:{port}")
    should_continue = True

    while should_continue:
        client_socket, addr = server_socket.accept()
        print(f" [x] Connection from {addr}")
        
        msg_type = int.from_bytes(client_socket.recv(1), "big")
        filename_len = int.from_bytes(client_socket.recv(1), "big")
        filename = client_socket.recv(filename_len)
        header_len = int.from_bytes(client_socket.recv(4), "big")
        header = client_socket.recv(header_len)

        print(f"msg_type = {msg_type}")
        print(f"filename_len = {filename_len}")
        print(f"filename = {filename}")
        print(f"header_len = {header_len}")
        print(f"header = {header}")



def main():
    gateway = Gateway("0.0.0.0", 9999)
    gateway.start()

main()
