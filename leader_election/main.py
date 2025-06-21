import os
from src.leader_election import LeaderElectionParticipant


def main():
    id = int(os.getenv("NODE_ID"))
    number_of_peers = int(os.getenv("CLUSTER_SIZE"))
    participant = LeaderElectionParticipant(id, number_of_peers)
    participant.run()


if __name__ == "__main__":
    main()
