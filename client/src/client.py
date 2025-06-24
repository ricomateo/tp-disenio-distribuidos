import time
import signal
import json
import os
from src.protocol import Protocol
from common.protocol_constants import QUERY_RESULT_MSG_TYPE, FIN_MSG_TYPE


MOVIES_FILENAME = "movies_metadata.csv"
RATINGS_FILENAME = "ratings.csv"
CREDITS_FILENAME = "credits.csv"


class Client:
    def __init__(self, hosts: list[str], port: int, batch_size: int):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.hosts = hosts
        self.port = port
        self.protocol = None

        self.batch_size = batch_size
        self.start_time = time.time()

        self.node_id = int(os.getenv("NODE_ID"))

    def connect_to_gateway(self):
        """
        Connect to the first gateway it finds available
        """
        for host in self.hosts:
            try:
                self.protocol = Protocol(host, self.port)
                return
            except Exception:
                continue
        # By this point, the client has tried to connect to all gateways
        # but none of them responded
        raise NoGatewaysAvailable("No gateways available")

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
        """
        Receives and prints the query results
        """
        results = {}
        while True:
            message = self.protocol.recv_message()
            if message["msg_type"] == QUERY_RESULT_MSG_TYPE:
                # Deserialize the response
                response = json.loads(message["result"])["response"]
                query_number = response["query"]
                if query_number in results:
                    continue
                result = response["result"]
                results[query_number] = result
                print_query_result(query_number, result)
            elif message["msg_type"] == FIN_MSG_TYPE:
                print("received finalization message, closing...")
                break
        # Save the results to a JSON file
        results_file_name = f"/app/output/results_{self.node_id}.json"
        with open(results_file_name, "w", encoding="utf-8") as f:
            data = json.dumps(results, ensure_ascii=False)
            f.write(data)

    def close(self):
        """
        Closes the protocol
        """

        end_time = time.time()
        elapsed_time = end_time - self.start_time
        print(
            f"Total time from connection to disconnection: {elapsed_time:.2f} seconds"
        )
        self.protocol.close()

    def _sigterm_handler(self, signum, _):
        """
        Sigterm handler
        """
        print("Received SIGTERM signal")
        print("Sending finalization message...")
        self.close()


def print_query_result(query_number, results):
    """
    Prints the query result as a table
    """
    print(f"========= QUERY {query_number} RESULTS =========")
    if query_number == 1:
        # Extract the genres names
        for row in results:
            genres = json.loads(row["genres"].replace("'", '"'))
            row["genres"] = [genre["name"] for genre in genres]
        print_table(["title", "genres"], results)

    elif query_number == 2:
        print_table(["country", "budget"], results)

    elif query_number == 3:
        print_table(["title", "rating"], results)

    elif query_number == 4:
        print_table(["name", "count"], results)

    elif query_number == 5:
        print_table(["feeling", "ratio"], results)

    print("\n")


def print_table(headers: list[str], data: dict):
    if not data:
        print("(no rows)")
        return

    # Calculate maximum column widths
    column_widths = {header: len(header) for header in headers}
    for row in data:
        for header in headers:
            value = str(row.get(header, ""))
            column_widths[header] = max(column_widths[header], len(value))

    header_line = " | ".join(header.ljust(column_widths[header]) for header in headers)
    print(header_line)
    print("-" * len(header_line))

    for row in data:
        row_values = []
        for header in headers:
            value = str(row.get(header, ""))
            row_values.append(value.ljust(column_widths[header]))
        print(" | ".join(row_values))


class NoGatewaysAvailable(Exception):
    """
    Error raised when there are no gateways available.
    """
