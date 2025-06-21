import socket

ELECTION = 1
LEADER = 2
PING = 3


class Protocol:
    def __init__(self, address, port, timeout):
        self.address = address
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        self.socket.bind(("0.0.0.0", 6969))
        self.socket.listen()

    def recv_message(self):
        peer_socket, _ = self.socket.accept()
        msg_type = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
        if msg_type == ELECTION:
            leader_id = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
            return {"msg_type": "election", "id": leader_id}
        elif msg_type == LEADER:
            leader_id = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
            return {"msg_type": "leader", "id": leader_id}
        elif msg_type == PING:
            return {"msg_type": "ping"}

    def send_election(self, address, leader_id: int):
        print(f"Enviando election a {address}...")
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, self.port))

        message_type = ELECTION.to_bytes(1, "big")
        leader_id = leader_id.to_bytes(1, "big")
        peer_socket.sendall(message_type)
        peer_socket.sendall(leader_id)
        print(f"Election enviada!")
        # TODO: check the close
        peer_socket.close()

    def send_leader(self, address, leader_id: int):
        print(f"Enviando leader a {address}...")
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, self.port))

        message_type = LEADER.to_bytes(1, "big")
        leader_id = leader_id.to_bytes(1, "big")
        peer_socket.sendall(message_type)
        peer_socket.sendall(leader_id)
        print(f"Leader enviado!")
        # TODO: check the close
        peer_socket.close()

    def send_ping(self, address):
        print(f"Enviando PING a {address}...")
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, self.port))

        message_type = PING.to_bytes(1, "big")
        peer_socket.sendall(message_type)
        print(f"PING enviado!")
        # TODO: check the close
        peer_socket.close()

    def set_timeout(self, timeout: int):
        try:
            self.socket.settimeout(timeout)
        except Exception as e:
            print(f"Failed to set timeout {timeout}. Error: {e}")

    def _recv_exact(self, peer_socket, n: int):
        """
        Reads exactly n bytes from the socket, and returns the data.
        If the connection is closed, raises an exception.
        """
        data = bytes()
        while len(data) < n:
            received_bytes = peer_socket.recv(n - len(data))
            if not received_bytes:
                raise ConnectionError("Connection closed")
            data += received_bytes
        return data
