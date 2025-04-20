from src.protocol import Protocol

class Client:
    def __init__(self, host: str, port: int, batch_size: int):
        self.protocol = Protocol(host, port)
        self.batch_size = batch_size
    
    def send_file(self, filepath: str):
        # Extraigo el filename del path
        filename = filepath.split('/')[-1]
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
        # TODO: indicar el nombre del archivo en el EOF
        self.protocol.send_end_of_file()

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
        for _ in range(5):
            result = self.protocol.recv_result()
            print(f"result = {result}")

    def close(self):
        self.protocol.close()
