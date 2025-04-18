from batcher import Batcher
from protocol import Protocol

class Client:
    def __init__(self, host: str, port: int, batch_size: int):
        self.protocol = Protocol(host, port)
        self.batch_size = batch_size
    
    def send_file(self, filepath: str):
        batcher = Batcher(filepath, self.batch_size)
        # Extract the filename from the path
        filename = filepath.split('/')[-1]
        # Read the first line
        header = batcher.get_line()

        self.protocol.send_file_header(filename, header)
        # TODO: send the file

        # Close the file
        batcher.stop()

def main():
    host = "0.0.0.0"
    port = 9999
    batch_size = 100
    client = Client(host, port, batch_size)
    client.send_file("../movies_metadata_reduced.csv")


main()
