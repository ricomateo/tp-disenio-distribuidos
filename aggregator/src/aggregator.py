# filter.py
import json
from common.middleware import Middleware
from common.packet import DataPacket, MoviePacket, QueryPacket, handle_final_packet, is_final_packet
from datetime import datetime
import os

class AggregatorNode:
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "sentiment_averages_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "deliver_queue")

        #self.exchange = os.getenv("RABBITMQ_EXCHANGE", "movie_exchange")
        #self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "sentiment_consumer")

        self.input_rabbitmq = Middleware(queue=self.input_queue)
        self.output_rabbitmq = Middleware(queue=self.output_queue)

        self.average_positive: tuple[float, int] = (0, 0)
        self.average_negative: tuple[float, int] = (0, 0)

        self.operation = os.getenv("operation", "total_invested")

        self.invested_per_country: dict[str, int] = {}

    def callback(self, ch, method, properties, body):
        try:
            # Recibir paquete
            packet_json = body.decode()
            
            header = json.loads(packet_json).get("header")
            if header and is_final_packet(header):
                if handle_final_packet(method, self.input_rabbitmq):
                    if self.operation == "total_invested":
                        for country, value in self.invested_per_country.items():
                            packet = DataPacket(
                                timestamp=datetime.utcnow().isoformat(),
                                data={
                                    "value": country,
                                    "total": value
                                }
                            )

                            self.output_rabbitmq.publish(packet.to_json())
                        self.output_rabbitmq.send_final()
                        self.input_rabbitmq.send_ack_and_close(method)
                    else:
                        averages = []

                        line_positive = " | ".join(["Positive average", str(self.average_positive[0]), str(self.average_positive[1])])
                        line_negative = " | ".join(["Negative average", str(self.average_negative[0]), str(self.average_negative[1])])

                        averages.append(line_positive)
                        averages.append(line_negative)

                        response_str = "\n".join(averages) if averages else "No se encontraron promedios."

                        query_packet = QueryPacket(
                            timestamp=datetime.utcnow().isoformat(),
                            data={"source": "aggregator"},
                            response=response_str
                        )
                        self.output_rabbitmq.publish(query_packet.to_json())

                        self.output_rabbitmq.send_final()
                        self.input_rabbitmq.send_ack_and_close(method)
                return
            
            packet = DataPacket.from_json(packet_json)

            # Procesar paquete (calcular promedio y ponerlo en un diccionario con los campos id, promedio y cantidad)

            if self.operation == "total_invested":
                print(f"packet.data es {packet.data}")

                country = packet.data["value"]
                invested = packet.data["total"]

                current_invested = self.invested_per_country.get(country, 0)
                self.invested_per_country[country] = current_invested + invested
            else:
                packet_data = [x for x in packet.data]
                sentiment = packet_data[0]
                average = float(packet_data[1])
                count = int(packet_data[2])

                if sentiment == "POS":                
                    new_count = self.average_positive[1] + count
                    new_average = (self.average_positive[0] * self.average_positive[1] + average) / new_count
                    self.average_positive = (new_average, new_count)
                    print(f"[updated positive number - current positive average: {self.average_positive}")
                else:
                    new_count = self.average_negative[1] + count
                    new_average = (self.average_negative[0] * self.average_negative[1] + average) / new_count
                    self.average_negative = (new_average, new_count)
                    print(f"[updated negative number - current negative average: {self.average_negative}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] operation is {self.operation}    Error processing message: {e}, raw packet is {packet_json}")
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
