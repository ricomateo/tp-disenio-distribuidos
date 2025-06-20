import time
import signal
import time
import json
import os
from src.protocol import Protocol
from common.protocol_constants import QUERY_RESULT_MSG_TYPE, FIN_MSG_TYPE


MOVIES_FILENAME = "movies_metadata.csv"
RATINGS_FILENAME = "ratings.csv"
CREDITS_FILENAME = "credits.csv"


class Client:
    def __init__(self, host: str, port: int, batch_size: int):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.protocol = Protocol(host, port)
        self.batch_size = batch_size
        self.start_time = time.time()

        self.node_id = int(os.getenv("NODE_ID"))

    def send_movies_file(self, filepath: str):
        filename = MOVIES_FILENAME
        self.send_file(filename, filepath)

    def send_ratings_file(self, filepath: str):
        filename = RATINGS_FILENAME
        self.send_file(filename, filepath)

    def send_credits_file(self, filepath: str):
        filename = CREDITS_FILENAME
        self.send_file(filename, filepath)

    def send_file(self, filename: str, filepath: str):
        try:
            with open(filepath, "rb") as file:
                # Leo el header del archivo
                header = file.readline().decode("utf-8")

                # Envio el header
                self.protocol.send_file_header(filename, header)

                # Envio el archivo en batches
                batch = self.read_batch(file)
                while len(batch) > 0:
                    self.protocol.send_file_batch(filename, batch)
                    batch = self.read_batch(file)

            # Envio EOF
            self.protocol.send_end_of_file(filename)
            print(f"Sent {filename} file")
        except Exception as e:
            raise Exception(f"Failed to send file {filename}. Error: {e}")

    def read_batch(self, file):
        batch = []
        for _ in range(self.batch_size):
            line = file.readline()
            if line == b"":
                break
            batch.append(line)
        return batch

    def send_finalization(self):
        self.protocol.send_finalization()

    def print_results(self):
        results = {}
        while True:
            message = self.protocol.recv_message()
            if message["msg_type"] == QUERY_RESULT_MSG_TYPE:
                response = json.loads(message["result"])["response"]
                query_number = response["query"]
                result = response["result"]
                results[query_number] = result
                print_query_result(query_number, result)
            elif message["msg_type"] == FIN_MSG_TYPE:
                print("received finalization message, closing...")
                break
        results_file_name = f"/app/output/results_{self.node_id}.json"
        with open(results_file_name, "w", encoding="utf-8") as f:
            data = json.dumps(results, ensure_ascii=False)
            f.write(data)

    def close(self):
        end_time = time.time()
        elapsed_time = end_time - self.start_time
        print(
            f"Total time from connection to disconnection: {elapsed_time:.2f} seconds"
        )
        self.protocol.close()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        print(f"Sending finalization message...")
        self.close()


def print_query_result(query_number, results):
    """
    Prints the query result as a table
    """
    print(f"========= QUERY {query_number} RESULTS =========")
    if query_number == 1:
        print("title  |  genres")
        for row in results:
            title = row["title"]
            genres = row["genres"]
            print(f"{title}  |  {genres}")

    elif query_number == 2:
        print("country  |  budget")
        for row in results:
            country = row["country"]
            budget = row["budget"]
            print(f"{country} | {budget}")

    elif query_number == 3:
        print("title  |  rating")
        for row in results:
            title = row["title"]
            rating = row["rating"]
            print(f"{title}  |  {rating}")

    elif query_number == 4:
        print("name  |  count")
        for row in results:
            name = row["name"]
            count = row["count"]
            print(f"{name}  |  {count}")

    elif query_number == 5:
        print("feeling  |  ratio")
        for row in results:
            feeling = row["feeling"]
            ratio = row["ratio"]
            print(f"{feeling}  |  {ratio}")

    if len(results) == 0:
        print("(no rows)")
    print("\n")
