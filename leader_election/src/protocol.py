import socket

ELECTION = 1
LEADER = 2
PING = 3


class LeaderElectionProtocol:
    """
    Leader election protocol
    """

    def __init__(self, port, timeout):
        self.address = "0.0.0.0"
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout)
        self.socket.bind((self.address, self.port))
        self.socket.listen()

    def recv_message(self) -> dict:
        """
        Receives a single message
        """
        # TODO: check whether to cose the connection here
        peer_socket, _ = self.socket.accept()
        msg_type = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
        if msg_type == ELECTION:
            leader_id = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
            return {"msg_type": "election", "id": leader_id}
        if msg_type == LEADER:
            leader_id = int.from_bytes(self._recv_exact(peer_socket, 1), "big")
            return {"msg_type": "leader", "id": leader_id}
        if msg_type == PING:
            return {"msg_type": "ping"}
        else:
            return {"msg_type": msg_type}

    def send_election(self, address, leader_id: int):
        """
        Sends the ELECTION message to the given address
        """
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, self.port))

        message_type = ELECTION.to_bytes(1, "big")
        leader_id = leader_id.to_bytes(1, "big")
        peer_socket.sendall(message_type)
        peer_socket.sendall(leader_id)
        # TODO: check the close
        peer_socket.close()

    def send_leader(self, address, leader_id: int):
        """
        Sends the LEADER message to the given address
        """
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, self.port))

        message_type = LEADER.to_bytes(1, "big")
        leader_id = leader_id.to_bytes(1, "big")
        peer_socket.sendall(message_type)
        peer_socket.sendall(leader_id)
        # TODO: check the close
        peer_socket.close()

    def send_ping(self, address):
        """
        Sends the PING message to the given address
        """
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((address, self.port))

        message_type = PING.to_bytes(1, "big")
        peer_socket.sendall(message_type)
        # TODO: check the close
        peer_socket.close()

    def set_timeout(self, timeout: int):
        """
        Sets the given timeout to the socket
        """
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
