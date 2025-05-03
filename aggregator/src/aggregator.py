import json
from common.middleware import Middleware
from common.packet import DataPacket, handle_final_packet, is_final_packet
from datetime import datetime
import os
import signal

class AggregatorNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "sentiment_averages_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "deliver_queue")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
        self.output_rabbitmq = Middleware(queue=self.output_queue)

        self.operation = os.getenv("operation", "total_invested")

        self.average_positive_by_client_id: dict[int, tuple[float, int]] = {} #(0, 0)
        self.average_negative_by_client_id: dict[int, tuple[float, int]] = {} #(0, 0)
        self.invested_per_country_by_client_id: dict[int, dict[str, int]] = {}
        self.count_by_actors_by_client_id: dict[int, dict[str, int]] = {}

    def callback(self, ch, method, properties, body):
        try:
            if self.running == False:
                if self.input_rabbitmq.check_no_consumers():
                    self.output_rabbitmq.send_final()
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibir paquete y manejar el cierre en caso de ser un final packet
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            if header and is_final_packet(header):
                
                if handle_final_packet(method, self.input_rabbitmq):
                    if self.operation == "total_invested":
                        # Mando un paquete por país y después el final packet
                        for country, value in self.invested_per_country_by_client_id[client_id].items():
                            packet = DataPacket(
                                client_id=client_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={
                                    "value": country,
                                    "total": value
                                }
                            )
                            self.output_rabbitmq.publish(packet.to_json())
               
                        if client_id in self.invested_per_country_by_client_id:
                            del self.invested_per_country_by_client_id[client_id]
                 
                        self.output_rabbitmq.send_final(client_id=client_id)
                        self.input_rabbitmq.send_ack_and_close(method)
                    
                    elif self.operation == "average":
                        # En caso de tener al menos una película para ese sentimiento, publico
                        # ese paquete en la queue y después mando el final packet
                        if self.average_positive_by_client_id[client_id][1] > 0:
                            packet_pos = DataPacket(
                                client_id=client_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={
                                    "feeling": "POS",
                                    "ratio": round(self.average_positive_by_client_id[client_id][0], 4),
                                    "count": self.average_positive_by_client_id[client_id][1]
                                }
                            )
                            self.output_rabbitmq.publish(packet_pos.to_json())

                        if self.average_negative_by_client_id[client_id][1] > 0:
                            packet_neg = DataPacket(
                                client_id=client_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={
                                    "feeling": "NEG",
                                    "ratio": round(self.average_negative_by_client_id[client_id][0], 4),
                                    "count": self.average_negative_by_client_id[client_id][1]
                                }
                            )
                            self.output_rabbitmq.publish(packet_neg.to_json())
           
                        if client_id in self.average_positive_by_client_id:
                                del self.average_positive_by_client_id[client_id]
                    
                        if client_id in self.average_negative_by_client_id:
                                del self.average_negative_by_client_id[client_id]
            

                        self.output_rabbitmq.send_final(client_id=client_id)
                        self.input_rabbitmq.send_ack_and_close(method)
                    
                    elif self.operation == "count":
                        for actor, count in self.count_by_actors_by_client_id[client_id].items():
                            packet = DataPacket(
                                client_id=client_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={
                                    "value": actor,
                                    "count": count
                                }
                            )
                            self.output_rabbitmq.publish(packet.to_json())
         
                        if client_id in self.count_by_actors_by_client_id:
                                del self.count_by_actors_by_client_id[client_id]
               
                        self.output_rabbitmq.send_final(client_id=client_id)
                        self.input_rabbitmq.send_ack_and_close(method)
                return
            
            packet = DataPacket.from_json(packet_json)

            # Procesar paquete segun la operación en cuestion
            if self.operation == "total_invested":
                # Sumo al recuento de lo invertido para ese pais
                country = packet.data["value"]
                invested = packet.data["total"]
                
                if client_id not in self.invested_per_country_by_client_id:
                    self.invested_per_country_by_client_id[client_id] = {}

                current_invested = self.invested_per_country_by_client_id[client_id].get(country, 0)
                self.invested_per_country_by_client_id[client_id][country] = current_invested + invested
            elif self.operation == "average":
                # Calculo el promedio y actualizo el promedio para el sentimiento del que sea la película
                sentiment = packet.data["feeling"]
                average = float(packet.data["ratio"])
                count = int(packet.data["count"])

                if client_id not in self.average_positive_by_client_id:
                    self.average_positive_by_client_id[client_id] = (0, 0)

                if client_id not in self.average_negative_by_client_id:
                    self.average_negative_by_client_id[client_id] = (0, 0)

                if sentiment == "POS":
                    new_count = self.average_positive_by_client_id[client_id][1] + count
                    new_average = (self.average_positive_by_client_id[client_id][0] * self.average_positive_by_client_id[client_id][1] + average * count) / new_count
                    self.average_positive_by_client_id[client_id] = (new_average, new_count)
                    print(f"[updated positive number - current positive average: {self.average_positive_by_client_id[client_id]}")
                else:
                    new_count = self.average_negative_by_client_id[client_id][1] + count
                    new_average = (self.average_negative_by_client_id[client_id][0] * self.average_negative_by_client_id[client_id][1] + average * count) / new_count
                    self.average_negative_by_client_id[client_id] = (new_average, new_count)
                    print(f"[updated negative number - current negative average: {self.average_negative_by_client_id[client_id]}")
            elif self.operation == "count":
                actor = packet.data["value"]
                new_count_movies = packet.data["count"]
                if client_id not in self.count_by_actors_by_client_id:
                    self.count_by_actors_by_client_id[client_id] = {}

                count_movies = self.count_by_actors_by_client_id[client_id].get(actor, 0)
                self.count_by_actors_by_client_id[client_id][actor] = count_movies + new_count_movies
              

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] operation is {self.operation}    Error processing message: {e}, raw packet is {packet_json}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self):
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.close()

    def close(self):
        print(f"Closing queues")
        self.running = False
        self.input_rabbitmq.cancel_consumer()
    