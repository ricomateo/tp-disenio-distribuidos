import os
from src.client import Client

if __name__ == "__main__":
    host = os.getenv("GATEWAY_HOST")
    port = int(os.getenv("GATEWAY_PORT"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))

    client = Client(host, port, batch_size)
    try:
        client.send_file("movies_metadata.csv")
        client.print_results()
    except Exception as e:
        print(f"Failed to send file movies_metadata.csv. Error: {e}")
    finally:
        client.close()
        
        
