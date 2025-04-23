import os
from src.client import Client

if __name__ == "__main__":
    host = os.getenv("GATEWAY_HOST")
    port = int(os.getenv("GATEWAY_PORT"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))

    client = Client(host, port, batch_size)
    try:
        client.send_movies_file("movies_metadata.csv")
        client.send_credits_file("credits.csv")
        client.send_ratings_file("ratings_reduced.csv")
    except Exception as e:
        print(e)

    try:
        client.send_finalization()
    except Exception as e:
        print(f"Failed to send finalization message. Error: {e}")

    try:
        client.print_results()
    except Exception as e:
        print(f"Failed to receive results. Error: {e}")

    finally:
        client.close()
        
        
