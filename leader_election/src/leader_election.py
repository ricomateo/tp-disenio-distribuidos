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
        self.protocol = Protocol("a", self.port)
        self.participating = False
        self.peer_prefix = peer_prefix
        self.running = True
        print(f"am_i_leader = {self.am_i_leader}")

    def start(self):
        process = multiprocessing.Process(target=self.listen)
        process.start()
        time.sleep(1)
        self.start_election()

    def start_election(self):
        peer_id = self.id + 1
        if peer_id == self.number_of_peers:
            peer_id = 0
        peer_address = self.get_peer_address(peer_id)
        self.protocol.send_election(peer_address, 6969, self.id)

    def get_peer_address(self, peer_id: int):
        if peer_id == 0:
            return self.peer_prefix  # e.g. "gateway"
        return f"{self.peer_prefix}_{peer_id}"  # e.g "gateway_1"

    def listen(self):
        while self.running:
            message = self.protocol.recv_message()
            if is_election(message):
                leader_id = message.get("id")
                print(f"Recibo election con leader_id = {leader_id}")
            else:
                print(f"Recibo mensaje desconocido {message}")


def is_election(message):
    message_type = message.get("msg_type")
    return message_type == "election"
