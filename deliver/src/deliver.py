# deliver_node.py
import json
import os
from datetime import datetime
from common.packet import MoviePacket, QueryPacket, handle_final_packet, is_final_packet
from common.middleware import Middleware


class DeliverNode:
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "deliver_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "query_queue")
        self.keep_columns = [col.strip() for col in os.getenv("KEEP_COLUMNS", "").split(",") if col.strip()]
        
        self.input_rabbitmq = Middleware(queue=self.input_queue)
        self.output_rabbitmq = Middleware(queue=self.output_queue)
        
        self.collected_movies = []

    def callback(self, ch, method, properties, body):
        try:
            body_decoded = body.decode()
            
            if is_final_packet(json.loads(body_decoded).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    lines = []
                    for movie in self.collected_movies:
                        campos = []
                        for key in self.keep_columns:
                            value = movie.get(key, "")
                            if isinstance(value, list):
                                value = ", ".join(map(str, value))
                            campos.append(f"{key}: {value}")
                        line = " | ".join(campos)
                        lines.append(line)

                    response_str = "\n".join(lines) if lines else "No se encontraron películas."

                    query_packet = QueryPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data={"source": self.input_queue},
                        response=response_str
                    )
                    self.output_rabbitmq.publish(query_packet.to_json())
                    #self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return

            packet = MoviePacket.from_json(body_decoded)
            movie = packet.movie
            
            if self.keep_columns:
                filtered_movie = {k: v for k, v in movie.items() if k in self.keep_columns}
            else:
                filtered_movie = movie

            self.collected_movies.append(filtered_movie)
            print(f" [DeliverNode] Movie added: {filtered_movie}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [DeliverNode] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start_node(self):
        print(f" [~] DeliverNode listening on {self.input_queue}, will send to {self.output_queue}")
        if self.keep_columns:
            print(f" [~] Filtering movie fields: {self.keep_columns}")
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in deliver node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()    