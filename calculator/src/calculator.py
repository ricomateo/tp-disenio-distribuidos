# filter.py
import json
from common.middleware import Middleware
from common.packet import MoviePacket, QueryPacket, handle_final_packet, is_final_packet
from datetime import datetime
import os

class CalculatorNode:
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "sentiment_positive_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")

        #self.exchange = os.getenv("RABBITMQ_EXCHANGE", "movie_exchange")
        #self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "sentiment_consumer")

        self.input_rabbitmq = Middleware(queue=self.input_queue)
        self.output_rabbitmq = Middleware(queue=self.output_queue)

        self.average: tuple[float, int] = (0, 0)

    def callback(self, ch, method, properties, body):
        try:
            # Recibir paquete
            packet_json = body.decode()
            
            # print(f"[DEBUG] Raw packet received: {packet_json}")

            header = json.loads(packet_json).get("header")
            if header and is_final_packet(header):                
                if handle_final_packet(method, self.input_rabbitmq):
                    averages = []

                    feeling_str = ""
                    if self.input_queue == "sentiment_positive_queue":
                        feeling_str = "POS"
                    else:
                        feeling_str = "NEG"

                    line = " | ".join([feeling_str, str(self.average[0]), str(self.average[1])])

                    averages.append(line)

                    response_str = "\n".join(averages) if averages else "No se encontraron promedios."

                    query_packet = QueryPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data={"source": "calculator"},
                        response=response_str
                    )
                    self.output_rabbitmq.publish(query_packet.to_json())

                    self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            packet = MoviePacket.from_json(packet_json)
            movie = packet.movie

            # Procesar paquete (calcular promedio y ponerlo en un diccionario con los campos id, promedio y cantidad)

            titulo = movie["title"]

            try:
                budget = int(movie["budget"])
                revenue = float(movie["revenue"])
            except (ValueError, TypeError):
                print(f" [!] Invalid budget or revenue in movie '{titulo}', skipping...")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
                return

            if budget == 0:
                print(f"Skipped movie {titulo} because budget was zero")
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
                return

            ratio = revenue / budget

            new_count = self.average[1] + 1
            new_average = (self.average[0] * self.average[1] + ratio) / new_count

            self.average = (new_average, new_count)

            print(f"[input - {self.input_queue}] current average: {self.average}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}, raw packet is {packet_json}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def start_node(self):
        print(f" [~] Starting sentiment analyzer")

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()
