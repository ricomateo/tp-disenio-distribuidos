import os
from src.gateway import Gateway

if __name__ == "__main__":
    host = os.getenv("GATEWAY_HOST", '0.0.0.0')
    port = int(os.getenv("GATEWAY_PORT", '9999'))
    gateway = Gateway("0.0.0.0", 9999)
    gateway.start()
