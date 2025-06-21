import time
import socket
from src.protocol import Protocol

TIMEOUT = 2
LEADER_TIMEOUT = TIMEOUT * 0.75


class LeaderElectionParticipant:
    def __init__(self, id: int, number_of_peers: int, port: int, peer_prefix: str):
        self.id = id
        self.number_of_peers = number_of_peers
        self.port = port
        self.address = "0.0.0.0"
        self.protocol = Protocol(self.address, self.port, TIMEOUT)
        self.participating = False
        self.peer_prefix = peer_prefix
        self.running = True
        self.current_leader = None

    def start(self):
        # The node with ID 0 waits for all other nodes to start
        # and triggers an election to choose the starting leader
        if self.id == 0 and self.number_of_peers > 1:
            time.sleep(1)
            self.participating = True
            self.send_election(self.id)
        self.listen()

    def get_peer_address(self, peer_id: int):
        if peer_id == 0:
            return self.peer_prefix  # e.g. "gateway"
        return f"{self.peer_prefix}_{peer_id}"  # e.g "gateway_1"

    def listen(self):
        while self.running:
            try:
                message = self.protocol.recv_message()
            except socket.timeout:
                if self.am_i_leader():
                    self.broadcast_ping()
                    continue
                # If no PING messages are received within the given time,
                # trigger an election
                self.participating = True
                self.send_election(self.id)
            try:
                if is_election(message):
                    self.handle_election_message(message)
                elif is_leader(message):
                    self.handle_leader_message(message)
                    if self.am_i_leader():
                        self.protocol.set_timeout(LEADER_TIMEOUT)
                elif is_ping(message):
                    continue
                else:
                    print(f"Recibo mensaje desconocido {message}")
            except Exception as e:
                print(f"Failed to decode message '{message}'. Error: {e}")

    def handle_election_message(self, message):
        leader_id = message.get("id")
        print(f"Recibo election con leader_id = {leader_id}")
        if not self.participating:
            self.participating = True
            leader_id = max(self.id, leader_id)
            self.send_election(leader_id)

        elif self.participating:
            if leader_id == self.id:
                self.participating = False
                self.send_leader(leader_id)
            elif leader_id > self.id:
                self.send_election(leader_id)

    def handle_leader_message(self, message):
        leader_id = message.get("id")
        print(f"Recibo nuevo leader = {leader_id}")
        self.participating = False
        if leader_id != self.current_leader:
            self.send_leader(leader_id)
        self.current_leader = leader_id

    def send_election(self, id):
        """
        Sends ELECTION message to the first peer it finds alive in the ring order
        """
        peers_id_list = list(range(0, self.number_of_peers))
        peers_sorted_circularly = (
            peers_id_list[self.id + 1 :] + peers_id_list[: self.id + 1]
        )
        for peer_id in peers_sorted_circularly:
            peer_address = self.get_peer_address(peer_id)
            try:
                self.protocol.send_election(peer_address, id)
            except Exception as e:
                continue
            break

    def send_leader(self, id):
        """
        Sends LEADER message to the first peer it finds alive in the ring order
        """
        peers_addresses = self.get_peers_addresses()
        for peer in peers_addresses:
            try:
                self.protocol.send_leader(peer, id)
            except Exception as e:
                continue
            break

    def get_peers_addresses(self):
        """
        Returns the addresses of the peers sorted in a circular way, starting
        from the peer next to me.

        For example, if the cluster size is 5 (the id list is [0,1,2,3,4]) and my ID
        is 2, then the peers will be in the following order: [3,4,0,1,2].
        """
        peers_id_list = list(range(0, self.number_of_peers))
        peers_ids_sorted_circularly = (
            peers_id_list[self.id + 1 :] + peers_id_list[: self.id + 1]
        )
        peers_addresses = [
            self.get_peer_address(peer_id) for peer_id in peers_ids_sorted_circularly
        ]
        return peers_addresses

    def broadcast_ping(self):
        peers_addresses = self.get_peers_addresses()
        my_address = self.get_peer_address(self.id)
        for peer in peers_addresses:
            if peer == my_address:
                continue
            try:
                self.protocol.send_ping(peer)
            except Exception:
                pass

    def am_i_leader(self):
        return self.id == self.current_leader


def is_election(message):
    message_type = message.get("msg_type")
    return message_type == "election"


def is_leader(message):
    message_type = message.get("msg_type")
    return message_type == "leader"


def is_ping(message):
    message_type = message.get("msg_type")
    return message_type == "ping"
