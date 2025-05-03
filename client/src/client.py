
import time
import signal
import time
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
            with open(filepath, 'rb') as file:
                # Leo el header del archivo
                header = file.readline().decode('utf-8')
                
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
            if line == b'':
                break
            batch.append(line)
        return batch
    
    def send_finalization(self):
        self.protocol.send_finalization()
    
    def print_results(self):
        with open("/app/output/results.txt", "w") as f:
            while True:
                message = self.protocol.recv_message()
                if message["msg_type"] == QUERY_RESULT_MSG_TYPE:
                    result = message["result"]
                    print(f"{result}\n")
                    f.write(f"{result}\n")
                elif message["msg_type"] == FIN_MSG_TYPE:
                    print(f"received finalization message, closing...")
                    break

    def close(self):
        end_time = time.time()  
        elapsed_time = end_time - self.start_time  
        print(f"Total time from connection to disconnection: {elapsed_time:.2f} seconds")
        self.protocol.close()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        print(f"Sending finalization message...")
        self.protocol.send_finalization()
        self.close()
