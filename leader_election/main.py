import os
from src.leader_election import LeaderElectionParticipant


def main():
    id = int(os.getenv("NODE_ID"))
    number_of_peers = int(os.getenv("CLUSTER_SIZE"))
    peer_prefix = "peer"
    port = 7777
    participant = LeaderElectionParticipant(id, number_of_peers, port, peer_prefix)
    participant.start()


if __name__ == "__main__":
    main()
