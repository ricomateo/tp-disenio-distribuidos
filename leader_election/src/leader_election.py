import time


class LeaderElectionParticipant:
    def __init__(self, id: int, number_of_peers: int):
        self.id = id
        self.number_of_peers = number_of_peers
        self.am_i_leader = id == (number_of_peers - 1)
        print(f"am_i_leader = {self.am_i_leader}")

    def run(self):
        while True:
            time.sleep(10)
