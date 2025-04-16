# gateway.py
import socket
import json
import os
from common.middleware import Middleware
from common.packet import FinalPacket, handle_final_packet, is_final_packet
from datetime import datetime

def start_gateway(host = None, port = None):
    
    # Configurar middleware de RabbitMQ
    output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "csv_queue")
    input_queue = os.getenv("RABBITMQ_INPUT_QUEUE", "query_queue") 
    rabbitmq = Middleware(queue=output_queue)
    rabbitmq_receiver = Middleware(queue=input_queue)
    host = host or os.getenv('GATEWAY_HOST', '0.0.0.0')  # Escucha en todas las interfaces
    port = port or int(os.getenv('GATEWAY_PORT', '9999'))
    batch_size = int(os.getenv('BATCH_SIZE', '100'))    
    
    # Configurar socket TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f" [x] Gateway listening on {host}:{port}")
    should_continue = True
    try:
        while should_continue:
            client_socket, addr = server_socket.accept()
            print(f" [x] Connection from {addr}")
            
            # Recibir tamaño del archivo
            file_size = int(client_socket.recv(16).decode().strip())
            print(f"[>] Tamaño esperado del archivo CSV: {file_size} bytes")
            # Recibir datos del CSV
            buffer = b""
            header = None
            line_buffer = []
            
            while len(buffer) < file_size:
                data = client_socket.recv(9024)
                if not data:
                    break
                buffer += data

                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    decoded_line = line.decode("utf-8")

                    if header is None:
                        header = decoded_line
                        print(f"[>] Header recibido: {header}")
                    else:
                        line_buffer.append(decoded_line)

                        if len(line_buffer) >= batch_size:
                            message = {
                                #'type': tipo,
                                "header": header,
                                "rows": line_buffer
                            }
                            rabbitmq.publish(message)
                            print(f"[✓] Publicadas {len(line_buffer)} líneas.")
                            line_buffer = []  
                        
                
            if line_buffer:
                message = {
                    #'type': tipo,
                    "header": header,
                    "rows": line_buffer
                }
                rabbitmq.publish(message)
                print(f"[✓] Publicadas {len(line_buffer)} líneas finales.")
                
            print("[✓] Archivo CSV recibido correctamente.")
            client_socket.close()
            
            # Publicar el FINAL PACKET
            rabbitmq_receiver.purge()
            rabbitmq.send_final()
            
            print(" [GATEWAY] Now listening for filtered movies in query_queue...")
            
            def callback_reader(ch, method, properties, body):
                nonlocal should_continue
                try:
                    packet_json = body.decode()
                    
            
                    if is_final_packet(json.loads(packet_json).get("header")):
                        if handle_final_packet(method, rabbitmq_receiver):
                            rabbitmq.send_final()
                            rabbitmq_receiver.send_ack_and_close(method)
                        should_continue = False
                        return
                    
                    response_str = json.loads(packet_json).get("response")
                    if response_str:
                        print(" [GATEWAY - RESULT] Resultado final recibido:\n")
                        print(response_str)
                    else:
                        print(" [GATEWAY - RESULT] Packet recibido sin campo 'response'. Ignorado.")

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except json.JSONDecodeError as e:
                    print(f" [GATEWAY - RESULT] Error decoding JSON: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
                except Exception as e:
                    print(f" [GATEWAY - RESULT] Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

            rabbitmq_receiver.consume(callback_reader)
        
    finally:
        rabbitmq.close()
        rabbitmq_receiver.close()
        server_socket.close()