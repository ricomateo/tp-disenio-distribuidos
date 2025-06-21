import socket

PING = 1
PONG = 2
ELECTION = 3
LEADER = 4


class Protocol:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("0.0.0.0", 6969))
        self.socket.listen()

    def recv_message(self):
        peer_socket, address = self.socket.accept()
        msg_type = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
        print(f"RECEIVED MSG_TYPE = {msg_type}")
        if msg_type == ELECTION:
            leader_id = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
            print(f"Received id {leader_id}")
            return {"msg_type": "election", "id": leader_id}
        elif msg_type == LEADER:
            leader_id = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
            print(f"Received id {leader_id}")
            return {"msg_type": "leader", "id": leader_id}

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
