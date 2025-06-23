import socket

CLIENT_COUNT_REQUEST = 1 
 
class GatewayConnection:
    """
    Represents a connection between the leader gateway and a peer gateway.
    """

    def __init__(self):
        # Socket for communicating with the gateway leader
        self.gateway_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.gateway_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.host = "0.0.0.0"
        self.port = 7778
        self.gateway_socket.bind((self.host, self.port))
        self.gateway_socket.listen(1)

    def recv_client_count(self) -> int:
        gateway, _ = self.gateway_socket.accept()
        client_count = int.from_bytes(self._recv_exact(4, gateway), "big")
        gateway.close()
        return client_count

    def recv_replica_message(self) -> dict:
        gateway, address = self.gateway_socket.accept()
        msg_type = int.from_bytes(self._recv_exact(1, gateway), "big")
        if msg_type == CLIENT_COUNT_REQUEST:
            gateway.close()
            return {"msg_type": "client_count_request", "from": address}
        gateway.close()
        return {"msg_type": "unknown"}

    def send_client_count(self, address, client_count):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, 7778))

        client_count_as_bytes = client_count.to_bytes(4, "big")
        peer_socket.sendall(client_count_as_bytes)

        peer_socket.shutdown(socket.SHUT_RDWR)
        peer_socket.close()

    def send_client_count_request(self, address):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, 7778))

        client_count_as_bytes = CLIENT_COUNT_REQUEST.to_bytes(1, "big")
        peer_socket.sendall(client_count_as_bytes)

        peer_socket.shutdown(socket.SHUT_RDWR)
        peer_socket.close()

    def close(self):
        self.gateway_socket.shutdown(socket.SHUT_RDWR)
        self.gateway_socket.close()

    def _recv_exact(self, n: int, gateway_socket):
        """
        Reads exactly n bytes from the socket, and returns the data.
        If the connection is closed, raises an exception.
        """
        data = bytes()
        while len(data) < n:
            received_bytes = gateway_socket.recv(n - len(data))
            if not received_bytes:
                raise ConnectionError("Connection closed")
            data += received_bytes
        return data
