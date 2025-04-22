import os
from src.client import Client

if __name__ == "__main__":
    host = os.getenv("GATEWAY_HOST")
    port = int(os.getenv("GATEWAY_PORT"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))

    client = Client(host, port, batch_size)
    try:

        client.send_movies_file("movies_metadata.csv")
        client.send_ratings_file("ratings_reduced.csv")
        client.send_credits_file("credits_reduced.csv")
        client.send_finalization()

        client.print_results()
    except Exception as e:
        print(f"Failed to send file. Error: {e}")
    finally:
        client.close()
        
        
