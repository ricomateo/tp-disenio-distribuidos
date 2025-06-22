import time
import socket
import threading
from src.leader_election_protocol import LeaderElectionProtocol

DEFAULT_TIMEOUT = 1
LEADER_TIMEOUT = DEFAULT_TIMEOUT * 0.75


class LeaderElector:
    """
    A participant in the leader election algorithm
    """

    def __init__(
        self,
        peer_id: int,
        number_of_peers: int,
        port: int,
        peer_prefix: str,
        semaphore: threading.Semaphore,
    ):
        self.id = peer_id
        self.number_of_peers = number_of_peers
        self.protocol = LeaderElectionProtocol(port=port, timeout=DEFAULT_TIMEOUT)
        self.participating = False
        self.peer_prefix = peer_prefix
        self.running = True
        self.current_leader = None
        self.semaphore = semaphore
        self.released_semaphore = False

    def start(self):
        """
        Starts the participant node.

        If there are no peers (peers = 1), then the node sets itself as the leader.

        The node with id 0 waits for all other nodes to start and
        triggers an election to choose the starting leader.
        """
        print("starting leader elector")
        if self.number_of_peers == 1:
            self.current_leader = self.id
            self.semaphore.release()
            self.released_semaphore = True
        elif self.id == 0 and self.number_of_peers > 1:
            time.sleep(1)
            self.participating = True
            self.send_election(self.id)
        self.listen()

    def listen(self):
        """
        Listens for incoming messages.

        If the peers receive no messages within the given DEFAULT_TIMEOUT, an
        election is triggered.

        Every LEADER_TIMEOUT seconds, the leader broadcasts a PING to let the
        peers know he's alive (and to avoid the peers timeout to be triggered).
        """
        while self.running:
            try:
                message = self.protocol.recv_message()
            except socket.timeout:
                # When the leader times out, it sends PING messages
                # to the peers.
                if self.am_i_leader():
                    self.broadcast_ping()
                    continue

                # If no PING messages are received within the given time,
                # trigger an election (the leader may be dead)
                self.participating = True
                self.send_election(self.id)
                continue
            except Exception as e:
                print(f"Failed to receive message. Error: {e}")
                continue
            try:
                if is_election(message):
                    self.handle_election_message(message)
                elif is_leader(message):
                    self.handle_leader_message(message)
                    # After the leader is elected, set its timeout
                    # to the leader timeout (which is shorter than the default)
                    if self.am_i_leader():
                        self.protocol.set_timeout(LEADER_TIMEOUT)
                        if not self.released_semaphore:
                            self.semaphore.release()
                            print("Released the semaphore!")
                            self.released_semaphore = True
                elif is_ping(message):
                    continue
                else:
                    print(f"Recibo mensaje desconocido {message}")
            except Exception as e:
                print(f"Failed to decode message '{message}'. Error: {e}")

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
            except Exception:
                continue
            break

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
        """
        Handles the leader message.
        """
        leader_id = message.get("id")
        print(f"Recibo nuevo leader = {leader_id}")
        self.participating = False
        if leader_id != self.current_leader:
            self.send_leader(leader_id)
        self.current_leader = leader_id

    def send_leader(self, id):
        """
        Sends LEADER message to the first peer it finds alive in the ring order
        """
        peers_addresses = self.get_peers_addresses()
        for peer in peers_addresses:
            try:
                self.protocol.send_leader(peer, id)
            except Exception:
                continue
            break

    def broadcast_ping(self):
        """
        Sends a PING message to all the peers.
        Should only be called by the leader.
        """
        peers_addresses = self.get_peers_addresses()
        my_address = self.get_peer_address(self.id)
        for peer in peers_addresses:
            if peer == my_address:
                continue
            try:
                self.protocol.send_ping(peer)
            except Exception:
                pass

    def get_peers_addresses(self) -> list[str]:
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

    def am_i_leader(self) -> bool:
        """
        Returns whether am I the leader
        """
        return self.id == self.current_leader

    def get_peer_address(self, peer_id: int) -> str:
        """
        Returns the address of the peer by its prefix and id (assumes the node runs in Docker).
        """
        if peer_id == 0:
            return self.peer_prefix  # e.g. "gateway"
        return f"{self.peer_prefix}_{peer_id}"  # e.g "gateway_1"


def is_election(message):
    message_type = message.get("msg_type")
    return message_type == "election"


def is_leader(message):
    message_type = message.get("msg_type")
    return message_type == "leader"


def is_ping(message):
    message_type = message.get("msg_type")
    return message_type == "ping"
