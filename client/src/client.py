from src.protocol import Protocol

class Client:
    def __init__(self, host: str, port: int, batch_size: int):
        print(f"gateway host = {host}, gateway port = {port}")
        self.protocol = Protocol(host, port)
        self.batch_size = batch_size
    
    def send_file(self, filepath: str):
        # Extract the filename from the path
        filename = filepath.split('/')[-1]
        with open(filepath, 'rb') as file:
            header = file.readline().decode('utf-8')
            self.protocol.send_file_header(filename, header)
            batch = self.read_batch(file)
            while len(batch) > 0:
                self.protocol.send_file_batch(filename, batch)
                batch = self.read_batch(file)
        self.protocol.send_end_of_file()

    def read_batch(self, file):
        batch = []
        for _ in range(self.batch_size):
            line = file.readline()
            print(f"line = {line}")
            if line == b'':
                break
            batch.append(line)
        return batch
    
    def print_results(self):
        for _ in range(5):
            result = self.protocol.recv_result()
            print(f"result = {result}")

    def close(self):
        self.protocol.close()
