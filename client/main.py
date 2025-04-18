import os
from src.client2 import Client

if __name__ == "__main__":
    host = os.getenv("GATEWAY_HOST")
    port = int(os.getenv("GATEWAY_PORT"))
    batch_size = os.getenv("BATCH_SIZE", 100)
    client = Client(host, port, batch_size)
    client.send_file("movies_metadata_reduced.csv")
    client.print_results()
    client.close()
