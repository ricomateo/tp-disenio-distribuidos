import time
import socket
from src.protocol import Protocol
import multiprocessing


class LeaderElectionParticipant:
    def __init__(self, id: int, number_of_peers: int, port: int, peer_prefix: str):
        self.id = id
        self.number_of_peers = number_of_peers
        self.am_i_leader = id == (number_of_peers - 1)
        self.port = port
        self.address = "0.0.0.0"
        self.protocol = Protocol(self.address, self.port)
        self.participating = False
        self.peer_prefix = peer_prefix
        self.running = True
        print(f"am_i_leader = {self.am_i_leader}")

    def start(self):
        process = multiprocessing.Process(target=self.listen)
        process.start()
        self.start_election()

    def start_election(self):
        self.participating = True
        peer_id = self.id + 1
        if peer_id == self.number_of_peers:
            peer_id = 0
        peer_address = self.get_peer_address(peer_id)
        peer_port = self.port
        self.protocol.send_election(peer_address, peer_port, self.id)

    def get_peer_address(self, peer_id: int):
        if peer_id == 0:
            return self.peer_prefix  # e.g. "gateway"
        return f"{self.peer_prefix}_{peer_id}"  # e.g "gateway_1"

    def listen(self):
        while self.running:
            message = self.protocol.recv_message()
            if is_election(message):
                self.handle_election_message(message)
            else:
                print(f"Recibo mensaje desconocido {message}")

    def get_peer_next_to(self, peer_id):
        next_peer_id = peer_id + 1
        if next_peer_id == self.number_of_peers:
            next_peer_id = 0
        return self.get_peer_address(next_peer_id)

    def handle_election_message(self, message):
        leader_id = message.get("id")
        print(f"Recibo election con leader_id = {leader_id}")
        if not self.participating:
            self.participating = True
            leader_id = max(self.id, leader_id)
            # Get the address of the peer next to me
            peer_address = self.get_peer_next_to(self.id)
            peer_port = self.port
            self.protocol.send_election(peer_address, peer_port, leader_id)

        elif self.participating:
            if leader_id == self.id:
                print(f"soy el lider! id: {leader_id}")
            elif leader_id > self.id:
                peer_address = self.get_peer_next_to(self.id)
                peer_port = self.port
                self.protocol.send_election(peer_address, peer_port, leader_id)


def is_election(message):
    message_type = message.get("msg_type")
    return message_type == "election"
