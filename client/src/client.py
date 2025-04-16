# client.py
import socket
import os

def send_csv(file_path, host = None, port = None):
    # Crear socket TCP
    host = host or os.getenv('GATEWAY_HOST', 'gateway')  # Nombre del servicio en Docker
    port = port or int(os.getenv('GATEWAY_PORT', '9999'))
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    
    # Leer archivo CSV
    with open(file_path, 'rb') as f:
        csv_data = f.read()
    
    # Enviar tamaño del archivo primero
    file_size = len(csv_data)
    client_socket.send(str(file_size).encode().ljust(16))  # Asegura un tamaño fijo para el encabezado
    # Enviar datos
    short_write(client_socket, csv_data)
    
    print(f" [x] Sent CSV: {file_path}")
    client_socket.close()
    
def short_write(conn, data: bytes, chunk_size: int = 1024):
    total_sent = 0
    data_length = len(data)
    while total_sent < data_length:
        chunk_end = min(total_sent + chunk_size, data_length)
        chunk = data[total_sent:chunk_end]
        try:
            sent = conn.send(chunk)
            total_sent += sent
            # print(f" [x] Total Sent CSV: {total_sent}")
        except Exception as e:
            raise RuntimeError(f"Error al escribir en el socket: {e}")

