import os
import json
from src.client import Client

if __name__ == "__main__":
    hosts: list[str] = json.loads(os.getenv("GATEWAY_HOSTS"))
    port = int(os.getenv("GATEWAY_PORT"))
    batch_size = int(os.getenv("BATCH_SIZE", "1000"))

    client = Client(hosts, port, batch_size)
    try:
        client.send_movies_file("movies_metadata.csv")
        client.send_ratings_file("ratings.csv")
        client.send_credits_file("credits.csv")
    except OSError as e:
        if e.errno == 9:
            print(f"Socket closed. Exiting...")
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
        
        
