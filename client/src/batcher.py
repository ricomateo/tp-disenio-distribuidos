
class Batcher:
    def __init__(self, file, batch_size):
        self.file = open(file, 'rb')
        self.batch_size = batch_size

    def get_line(self) -> str:
        return self.file.readline().decode('utf-8')

    def get_batch(self) -> list[bytes]:
        batch = []
        for _ in range(self.batch_size):
            line = self.file.readline()
            if line == b'':
                break
            batch.append(line)
        return batch

    def stop(self):
        self.file.close()
