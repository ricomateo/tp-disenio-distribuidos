import socket
from common.protocol_constants import HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE

class Protocol:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((host, port))

    def send_file_header(self, filename: str, header: str):
        message_type = HEADER_MSG_TYPE
        filename_len = len(filename).to_bytes(1, "big")
        filename = filename.encode('utf-8')
        header_len = len(header).to_bytes(4, "big")
        header = header.encode('utf-8')

        self.server_socket.sendall(message_type.to_bytes(1, "big"))
        self.server_socket.sendall(filename_len)
        self.server_socket.sendall(filename)
        self.server_socket.sendall(header_len)
        self.server_socket.sendall(header)

    def send_file_batch(self, filename: str, batch: list[bytes]):
        message_type = BATCH_MSG_TYPE
        filename_len = len(filename).to_bytes(1, "big")
        filename = str(filename).encode('utf-8')
        batch_size = len(batch).to_bytes(4, "big")

        self.server_socket.sendall(message_type.to_bytes(1, "big"))
        self.server_socket.sendall(filename_len)
        self.server_socket.sendall(filename)
        self.server_socket.sendall(batch_size)
        
        for row in batch:
            row_len = len(row).to_bytes(4, "big")
            self.server_socket.sendall(row_len)
            self.server_socket.sendall(row)
    
    def send_end_of_file(self):
        message_type = EOF_MSG_TYPE
        self.server_socket.sendall(message_type.to_bytes(1, "big"))

    def recv_result(self):
        # TODO: agregar protocolo de recepcion de respuestas
        self.server_socket.recv(1024)

    def close(self):
        self.server_socket.close()
