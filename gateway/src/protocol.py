import socket
from common.protocol_constants import HEADER_MSG_TYPE, BATCH_MSG_TYPE, EOF_MSG_TYPE, FIN_MSG_TYPE, QUERY_RESULT_MSG_TYPE


class Protocol:
    def __init__(self, client_socket: socket.socket):
        """Inicializa el protocolo con un socket de cliente existente."""
        self.client_socket = client_socket

    def recv_message(self):
        msg_type = int.from_bytes(self._recv_exact(1), "big")
        print(f"RECEIVED MSG_TYPE = {msg_type}")
        if msg_type == HEADER_MSG_TYPE:
            filename_len = int.from_bytes(self._recv_exact(1), "big")
            filename = self._recv_exact(filename_len).decode('utf-8')
            header_len = int.from_bytes(self._recv_exact(4), "big")
            header = self._recv_exact(header_len).decode('utf-8')
            return {"msg_type": HEADER_MSG_TYPE, "filename": filename, "header": header}
        
        elif msg_type == BATCH_MSG_TYPE:
            filename_len = int.from_bytes(self._recv_exact(1), "big")
            filename = self._recv_exact(filename_len).decode('utf-8')
            batch_size = int.from_bytes(self._recv_exact(4), "big")
            rows = []
            for _ in range(batch_size):
                row_len = int.from_bytes(self._recv_exact(4), "big")
                row = self._recv_exact(row_len).decode('utf-8')
                # TODO: check if it is required to split the row
                rows.append(row)
            return {"msg_type": BATCH_MSG_TYPE, "filename": filename, "rows": rows}
        
        elif msg_type == EOF_MSG_TYPE:
            filename_len = int.from_bytes(self._recv_exact(1), "big")
            filename = self._recv_exact(filename_len).decode('utf-8')
            return {"msg_type": EOF_MSG_TYPE, "header": "EOF", "filename": filename}
        
        elif msg_type == FIN_MSG_TYPE:
            return {"msg_type": FIN_MSG_TYPE}

    def send_result(self, result: str):
        message_type = QUERY_RESULT_MSG_TYPE.to_bytes(1, "big")
        result = str(result).encode('utf-8')
        result_len = len(result).to_bytes(4, "big")

        self.client_socket.sendall(message_type)
        self.client_socket.sendall(result_len)
        self.client_socket.sendall(result)

    def send_finalization(self):
        message_type = FIN_MSG_TYPE.to_bytes(1, "big")
        self.client_socket.sendall(message_type)

    def _recv_exact(self, n: int):
        """
        Reads exactly n bytes from the socket, and returns the data.
        If the connection is closed, raises an exception.
        """
        data = bytes()
        while len(data) < n:
            received_bytes = self.client_socket.recv(n - len(data))
            if not received_bytes:
                raise ConnectionError("Connection closed")
            data += received_bytes
        return data

    def close(self):
        if self.client_socket is not None:
            self.client_socket.shutdown(socket.SHUT_RDWR)
            self.client_socket.close()
