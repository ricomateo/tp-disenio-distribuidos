import time
import socket
import threading
import logging
import os
from common.atomic_write import atomic_write
from src.leader_election_protocol import LeaderElectionProtocol

# This is required to mute the pika logging
logging.getLogger("pika").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="LEADER_ELECTION - [%(levelname)s] %(message)s")

DEFAULT_TIMEOUT = 2
LEADER_TIMEOUT = DEFAULT_TIMEOUT * 0.2

LEADER_FILE = "leader"


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
        self.current_leader_lock = threading.Lock()
        self.current_leader = None
        self.semaphore = semaphore
        self.process = threading.Thread(target=self.start)
        self.process.start()

    def start(self):
        """
        Starts the participant node.

        If there are no peers (peers = 1), then the node sets itself as the leader.

        The node with id 0 waits for all other nodes to start and
        triggers an election to choose the starting leader.
        """
        if self.number_of_peers == 1:
            logging.info("[LEADER_ELECTION] Setting myself as the leader since there are no peers")
            self.set_current_leader(self.id)
        elif self.should_trigger_election():
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
                logging.info(
                    "Current leader %s is dead. Starting new election.",
                    self.current_leader
                )
                self.participating = True
                self.send_election(self.id)
                continue
            except Exception as e:
                logging.debug("[LEADER_ELECTION] Failed to receive message. Error: %s", e)
                continue
            try:
                if is_election(message):
                    self.handle_election_message(message)
                elif is_leader(message):
                    self.handle_leader_message(message)
                    # After the leader is elected, set its timeout
                    # to the leader timeout (which is shorter than the default)
                    if self.am_i_leader():
                        self.broadcast_ping()
                        self.protocol.set_timeout(LEADER_TIMEOUT)
                elif is_ping(message):
                    if self.current_leader is None:
                        leader_id = message.get("id")
                        if leader_id is not None:
                            self.set_current_leader(leader_id)
                            logging.info("[LEADER_ELECTION] Received leader id %s", self.current_leader)
                            self.send_leader(leader_id)
                    continue
                else:
                    logging.debug("[LEADER_ELECTION] Received unknown message %s.", message)
            except Exception as e:
                logging.warning("[LEADER_ELECTION] Failed to decode message %s. Error: %s", message, e)

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
                logging.info("[LEADER_ELECTION] Sent ELECTION message with ID %s to peer %s", id, peer_address)
            except Exception:
                continue
            break

    def handle_election_message(self, message):
        leader_id = message.get("id")
        logging.info("[LEADER_ELECTION] Received ELECTION message with leader ID: %s", leader_id)
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
        self.participating = False
        if leader_id == self.current_leader:
            return
        logging.info("[LEADER_ELECTION] New leader elected with ID: %s", leader_id)
        self.set_current_leader(leader_id)
        self.send_leader(leader_id)

    def send_leader(self, id):
        """
        Sends LEADER message to the first peer it finds alive in the ring order
        """
        peers_addresses = self.get_peers_addresses()
        for peer in peers_addresses:
            try:
                self.protocol.send_leader(peer, id)
                logging.info("[LEADER_ELECTION] Sent LEADER message with ID %s to peer %s", id, peer)
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
                self.protocol.send_ping(peer, self.id)
            except Exception:
                pass

    def set_current_leader(self, leader_id):
        """
        Sets the current leader as leader_id and signals the semaphore
        """
        self.participating = False
        with self.current_leader_lock:
            self.current_leader = leader_id
        self.semaphore.release()
        atomic_write(LEADER_FILE, str(leader_id))

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

    def should_trigger_election(self):
        previous_leader = os.path.exists(LEADER_FILE)
        if previous_leader:
            return False
        return self.id == 0 and self.number_of_peers > 1


    def close(self):
        self.running = False
        self.protocol.close()
        logging.shutdown()
        self.process.join()


def is_election(message):
    message_type = message.get("msg_type")
    return message_type == "election"


def is_leader(message):
    message_type = message.get("msg_type")
    return message_type == "leader"


def is_ping(message):
    message_type = message.get("msg_type")
    return message_type == "ping"
