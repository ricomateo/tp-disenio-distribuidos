import json
import os
from datetime import datetime
import signal
from common.leader_queue import LeaderQueue
from common.packet import DataPacket, QueryPacket, is_final_packet
from common.middleware import Middleware
from common.worker_protocol import WorkerProtocol


class DeliverNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "deliver_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "query_queue")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.input_rabbitmq = Middleware(
            queue=self.input_queue, consumer_tag=self.consumer_tag
        )
        self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE", "final_deliver")
        self.query_number = int(os.getenv("QUERY_NUMBER", "1"))
        self.final_rabbitmq = Middleware(
            queue=self.final_queue, consumer_tag=self.consumer_tag
        )
        self.cluster_size = int(os.getenv("CLUSTER_SIZE", ""))
        self.leader_queue = None
        if int(self.query_number) == 5:
            self.leader_queue = LeaderQueue(
                self.final_queue,
                "",
                self.consumer_tag,
                self.cluster_size,
                output_exchange=self.output_exchange,
            )
        self.response_by_client = {}
        self.control = WorkerProtocol(
            self.health_server_ip, self.health_server_port, self.health_server_port
        )

    def callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibo el paquete, si es el último mando los resultados
            body_decoded = body.decode()
            packet = json.loads(body_decoded)
            if is_final_packet(packet.get("header")):
                client_id = packet.get("client_id")
                final_response = self.generate_final_response(client_id)
                final_response_str = json.dumps(
                    {"response": final_response}, ensure_ascii=False
                )
                print(f"final response = {json.dumps(final_response, indent=4)}")
                query_packet = QueryPacket(
                    timestamp=datetime.utcnow().isoformat(), response=final_response_str
                )
                self.output_rabbitmq.confirm_delivery()
                self.output_rabbitmq.publish(query_packet.to_json(), str(client_id))
                self.final_rabbitmq.send_final_with_node_id(
                    client_id=int(client_id), node_id=self.query_number, count=0
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            packet = DataPacket.from_json(body_decoded)
            self.process_packet(packet)

            print(f" [DeliverNode] Movie added with id: {packet.client_id}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [DeliverNode] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def process_packet(self, packet: DataPacket):
        try:
            client_id = packet.client_id
            data = packet.data
            if client_id not in self.response_by_client:
                self.response_by_client[client_id] = []
            self.response_by_client[client_id].append(data)
        except Exception as e:
            print(f"alerta por subnormal: {e}")

    def generate_final_response(self, client_id):
        """
        Generates the dictionary containing the responses for the given client.
        """
        response = {"query": self.query_number, "result": []}

        if self.query_number == 1:
            response["result"] = self.response_by_client.get(client_id, [])

        elif self.query_number == 2:
            top_5_countries = sorted(
                self.response_by_client.get(client_id, []),
                key=lambda d: d["total"],
                reverse=True,
            )[:5]  # Get top 5
            # Rename the keys
            for register in top_5_countries:
                register["country"] = register.pop("value")
                register["budget"] = register.pop("total")
            response["result"] = top_5_countries

        elif self.query_number == 3:
            keys_to_keep = ["id", "title", "average", "count"]
            ratings = self.response_by_client.get(client_id, [])
            filtered_ratings = []
            # Remove unnecessary keys
            for rating in ratings:
                filtered_rating = {
                    key: value for key, value in rating.items() if key in keys_to_keep
                }
                filtered_ratings.append(filtered_rating)
            sorted_ratings = sorted(
                filtered_ratings,
                key=lambda d: d["average"],
            )
            if len(sorted_ratings) > 0:
                worst_rating = sorted_ratings[0]
                best_rating = sorted_ratings[-1]
                # Rename the keys
                worst_rating["rating"] = worst_rating.pop("average")
                best_rating["rating"] = best_rating.pop("average")
                response["result"] = [best_rating, worst_rating]

        elif self.query_number == 4:
            top_10_actors = sorted(
                self.response_by_client.get(client_id, []),
                key=lambda k: (-k["count"], k["value"]),
            )[:10]  # Get top 10
            for actor in top_10_actors:
                # Rename "value" to "name"
                actor["name"] = actor.pop("value")
            response["result"] = top_10_actors

        elif self.query_number == 5:
            sentiment_ratios = self.response_by_client.get(client_id, [])
            # Expand the feeling value
            for ratio in sentiment_ratios:
                if ratio.get("feeling") == "POS":
                    ratio["feeling"] = "POSITIVE"
                elif ratio.get("feeling") == "NEG":
                    ratio["feeling"] = "NEGATIVE"
            response["result"] = sentiment_ratios

        return response

    def _log_startup(self):
        """Log startup information about queues, filters, and columns."""
        print(
            f" [~] DeliverNode listening on {self.input_queue}, will send to {self.output_queue}"
        )

    def start_node(self):
        self._log_startup()
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in deliver node: {e}")
        finally:
            if self.leader_queue:
                self.leader_queue.join()
            self.close()

    def _sigterm_handler(self, signum, _):
        print("Received SIGTERM signal")
        self.running = False
        if self.control:
            self.control.stop()
        if self.input_rabbitmq:
            self.input_rabbitmq.cancel_consumer()
        if self.leader_queue:
            self.leader_queue.close()

    def close(self):
        print("Closing queues")
        if self.leader_queue:
            self.leader_queue.close()
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
        if self.final_rabbitmq:
            self.final_rabbitmq.close()
