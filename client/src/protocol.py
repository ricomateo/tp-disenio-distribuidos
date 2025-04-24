import socket
from common.protocol_constants import HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE, FIN_MSG_TYPE, QUERY_RESULT_MSG_TYPE

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
    
    def send_end_of_file(self, filename: str):
        message_type = EOF_MSG_TYPE
        filename_len = len(filename).to_bytes(1, "big")
        filename = str(filename).encode('utf-8')
        
        self.server_socket.sendall(message_type.to_bytes(1, "big"))
        self.server_socket.sendall(filename_len)
        self.server_socket.sendall(filename)

    def send_finalization(self):
        message_type = FIN_MSG_TYPE
        self.server_socket.sendall(message_type.to_bytes(1, "big"))

    def close(self):
        try:
            self.server_socket.shutdown(socket.SHUT_RDWR)
            self.server_socket.close()
        except OSError:
            print(f"Socket already closed")

    def recv_message(self):
        msg_type = int.from_bytes(self._recv_exact(1), "big")
        if msg_type == QUERY_RESULT_MSG_TYPE:
            result_len = int.from_bytes(self._recv_exact(4), "big")
            result = self._recv_exact(result_len).decode('utf-8')
            return {"msg_type": msg_type, "result": result}
        elif msg_type == FIN_MSG_TYPE:
            return {"msg_type": msg_type}
        else:
            print(f"Received unexpected message type: {msg_type}")
            return {"msg_type": msg_type}

    def _recv_exact(self, n: int):
        """
        Reads exactly n bytes from the socket, and returns the data.
        If the connection is closed, raises an exception.
        """
        data = bytes()
        while len(data) < n:
            received_bytes = self.server_socket.recv(n - len(data))
            if not received_bytes:
                raise ConnectionError("Connection closed")
            data += received_bytes
        return data
