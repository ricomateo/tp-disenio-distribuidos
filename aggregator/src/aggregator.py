import json
from datetime import datetime
import os
import signal
import glob
import logging
from common.logger import init_logging
from common.middleware import Middleware
from common.packet import DataPacket, is_delete_packet, is_final_packet
from common.atomic_write import atomic_write
from common.worker_protocol import WorkerProtocol
from common.dead_clients_tracker import DeadClientsTracker

init_logging(os.getenv("LOG_LEVEL", "info"))


class AggregatorNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "sentiment_averages_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "deliver_queue")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.input_rabbitmq = Middleware(
            queue=self.input_queue, consumer_tag=self.consumer_tag
        )
        self.output_rabbitmq = Middleware(queue=self.output_queue)
       

        self.operation = os.getenv("operation", "total_invested")
        
        self.state_dir = f"../data/{self.output_queue}"
        os.makedirs(self.state_dir, exist_ok=True)
        
        self.dead_clients_tracker = DeadClientsTracker(is_join_node=False, node_id=0, state_dir=self.state_dir)
        
        self.average_positive_by_client_id: dict[int, tuple[float, int]] = {}  # (0, 0)
        self.average_negative_by_client_id: dict[int, tuple[float, int]] = {}  # (0, 0)
        self.invested_per_country_by_client_id: dict[int, dict[str, int]] = {}
        self.count_by_actors_by_client_id: dict[int, dict[str, int]] = {}

        self.processed_messages_by_client = {}  # client_id to set of messages ids
        self.control = WorkerProtocol(
            self.health_server_ip, self.health_server_port, self.health_server_port
        )

    def callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibir paquete y manejar el cierre en caso de ser un final packet
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")

            if self.dead_clients_tracker.client_is_dead(client_id):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if header and is_delete_packet(header):
                logging.info("Received DELETE packet for client %s", client_id)
                self.output_rabbitmq.send_delete(client_id=client_id)
                logging.info("Sent DELETE packet for client %s", client_id)
                self.dead_clients_tracker.set_client_as_dead(client_id)
                self.delete_client(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if header and is_final_packet(header):
                if self.dead_clients_tracker.client_is_dead(client_id):
                    logging.info(
                        "Received FINAL packet for dead client %s, ignoring it...",
                        client_id,
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                logging.info("Received FINAL packet for client %s", client_id)
                self.send_results(client_id)
                logging.info("Sent results for client %s", client_id)
                self.output_rabbitmq.send_final(client_id=client_id)
                logging.info("Sent FINAL packet for client %s", client_id)
                self.dead_clients_tracker.set_client_as_dead(client_id)
                self.delete_client(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            packet = DataPacket.from_json(packet_json)

            # Initialize processed messages set
            if client_id not in self.processed_messages_by_client:
                self.processed_messages_by_client[client_id] = set()

            # Skip duplicate messages
            if packet.id in self.processed_messages_by_client[client_id]:
                logging.debug("Duplicate packet with ID %s", packet.id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            # Process the packet
            self.process_packet(packet, client_id)
            # Set the message as processed
            self.processed_messages_by_client[client_id].add(packet.id)
            # Save the state
            self.save_state(client_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.debug("Message %s acknowledged", method.delivery_tag)

        except json.JSONDecodeError as e:
            logging.warning("Error decoding JSON: %s", e)
            ch.basic_nack(
                delivery_tag=method.delivery_tag, multiple=False, requeue=False
            )
        except Exception as e:
            logging.warning(
                "Error processing message: %s, raw packet is %s", e, packet_json
            )
            ch.basic_nack(
                delivery_tag=method.delivery_tag, multiple=False, requeue=False
            )

    def start_node(self):
        """
        Starts the node
        """
        self.load_state()
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            logging.error("Error in aggregator node: %s", e)
        finally:
            self.close()

    def process_packet(self, packet, client_id):
        """
        Process the given packet for the given client_id
        and updates its state.
        """
        # Procesar paquete segun la operación en cuestion
        if self.operation == "total_invested":
            # Sumo al recuento de lo invertido para ese pais
            country = packet.data["value"]
            invested = packet.data["total"]

            if client_id not in self.invested_per_country_by_client_id:
                self.invested_per_country_by_client_id[client_id] = {}

            current_invested = self.invested_per_country_by_client_id[client_id].get(
                country, 0
            )
            self.invested_per_country_by_client_id[client_id][country] = (
                current_invested + invested
            )
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
                new_average = (
                    self.average_positive_by_client_id[client_id][0]
                    * self.average_positive_by_client_id[client_id][1]
                    + average * count
                ) / new_count
                self.average_positive_by_client_id[client_id] = (new_average, new_count)
                logging.debug(
                    "updated positive number - current positive average: %s",
                    self.average_positive_by_client_id[client_id],
                )
            else:
                new_count = self.average_negative_by_client_id[client_id][1] + count
                new_average = (
                    self.average_negative_by_client_id[client_id][0]
                    * self.average_negative_by_client_id[client_id][1]
                    + average * count
                ) / new_count
                self.average_negative_by_client_id[client_id] = (new_average, new_count)
                logging.debug(
                    "updated negative number - current negative average: %s",
                    self.average_negative_by_client_id[client_id],
                )
        elif self.operation == "count":
            actor = packet.data["value"]
            new_count_movies = packet.data["count"]
            if client_id not in self.count_by_actors_by_client_id:
                self.count_by_actors_by_client_id[client_id] = {}

            count_movies = self.count_by_actors_by_client_id[client_id].get(actor, 0)
            self.count_by_actors_by_client_id[client_id][actor] = (
                count_movies + new_count_movies
            )

    def delete_client(self, client_id):
        """
        Deletes the state for the given client.
        """
        if self.operation == "total_invested":
            if client_id in self.invested_per_country_by_client_id:
                del self.invested_per_country_by_client_id[client_id]

        elif self.operation == "average":
            if client_id in self.average_positive_by_client_id:
                del self.average_positive_by_client_id[client_id]
            if client_id in self.average_negative_by_client_id:
                del self.average_negative_by_client_id[client_id]

        elif self.operation == "count":
            if client_id in self.count_by_actors_by_client_id:
                del self.count_by_actors_by_client_id[client_id]

        if client_id in self.processed_messages_by_client:
            del self.processed_messages_by_client[client_id]
            
        try:
            file_path = os.path.join(self.state_dir, f"client.{client_id}.json")
            os.remove(file_path)
            logging.info("Deleted client %s data", client_id)
        except Exception as e:
            logging.warning(
                "Failed to remove file for client %s. Error: %s", client_id, e
            )

    def save_state(self, client_id):
        """
        Saves the state by writing (atomically) it to the hard drive.
        """
        state = self.get_state(client_id)
        processed_messages = list(self.processed_messages_by_client.get(client_id, []))
        filename = os.path.join(self.state_dir, f"client.{client_id}.json")
        data = json.dumps(
            {"state": state, "processed_messages": processed_messages},
            ensure_ascii=False,
        )
        # Save the state (atomically) to a file
        atomic_write(filename, data)

    def get_state(self, client_id):
        """
        Returns the state of the given client.
        """
        if self.operation == "total_invested":
            if client_id in self.invested_per_country_by_client_id:
                return self.invested_per_country_by_client_id[client_id]

        elif self.operation == "average":
            state = {"average_positive": (0, 0), "average_negative": (0, 0)}
            if client_id in self.average_positive_by_client_id:
                state["average_positive"] = self.average_positive_by_client_id[
                    client_id
                ]
            if client_id in self.average_negative_by_client_id:
                state["average_negative"] = self.average_negative_by_client_id[
                    client_id
                ]
            return state

        elif self.operation == "count":
            if client_id in self.count_by_actors_by_client_id:
                return self.count_by_actors_by_client_id[client_id]
        # Default empty state
        return {}

    def set_state(self, client_id, state):
        """
        Sets the given state to the corresponding variable.
        """
        if self.operation == "total_invested":
            self.invested_per_country_by_client_id[client_id] = state

        elif self.operation == "average":
            self.average_positive_by_client_id[client_id] = state["average_positive"]
            self.average_negative_by_client_id[client_id] = state["average_negative"]

        elif self.operation == "count":
            self.count_by_actors_by_client_id[client_id] = state

    def send_results(self, client_id):
        """
        Sends the aggregated results for the given client_id.
        """
        if self.operation == "total_invested":
            count = 0
            # Mando un paquete por país y después el final packet
            for country, value in self.invested_per_country_by_client_id.get(client_id, {}).items():
                packet = DataPacket(
                    client_id=client_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data={"value": country, "total": value},
                    id=f"{client_id}-{count}",
                )
                self.output_rabbitmq.publish(packet.to_json())
                count += 1

        elif self.operation == "average":
            # En caso de tener al menos una película para ese sentimiento, publico
            # ese paquete en la queue y después mando el final packet
            avg_pos = self.average_positive_by_client_id.get(client_id, (0, 0))
            if avg_pos[1] > 0:
                packet_pos = DataPacket(
                    client_id=client_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data={
                        "feeling": "POS",
                        "ratio": round(
                            self.average_positive_by_client_id[client_id][0], 4
                        ),
                        "count": self.average_positive_by_client_id[client_id][1],
                    },
                    id=f"{client_id}-1",
                )
                self.output_rabbitmq.publish(packet_pos.to_json())

            avg_neg = self.average_negative_by_client_id.get(client_id, (0, 0))
            if avg_neg[1] > 0:
                packet_neg = DataPacket(
                    client_id=client_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data={
                        "feeling": "NEG",
                        "ratio": round(
                            self.average_negative_by_client_id[client_id][0], 4
                        ),
                        "count": self.average_negative_by_client_id[client_id][1],
                    },
                    id=f"{client_id}-2",
                )
                self.output_rabbitmq.publish(packet_neg.to_json())

        elif self.operation == "count":
            packet_id = 0
            for actor, count in self.count_by_actors_by_client_id.get(client_id, {}).items():
                packet = DataPacket(
                    client_id=client_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data={"value": actor, "count": count},
                    id=f"{client_id}-{packet_id}",
                )
                self.output_rabbitmq.publish(packet.to_json())
                packet_id += 1

    def load_state(self):
        state_files = glob.glob(os.path.join(self.state_dir, "client.*.json"))
        if len(state_files) != 0:
            logging.info("Found state files: %s", state_files)
        for file in state_files:
            try:
                client_id = int(os.path.basename(file).split(".")[1])
                with open(file, "r", encoding="utf-8") as f:
                    data = json.loads(f.read())
                self.processed_messages_by_client[client_id] = set(
                    data.get("processed_messages", [])
                )
                state = data.get("state")
                self.set_state(client_id, state)
                logging.info(
                    "Recovered state from client %s, len(processed_messages) = %s",
                    client_id,
                    len(self.processed_messages_by_client[client_id]),
                )
            except Exception as e:
                logging.warning(
                    "Failed to recover state for client %s. Error: %s", client_id, e
                )

    def _sigterm_handler(self, signum, _):
        logging.info("Received SIGTERM signal")
        self.running = False
        if self.control:
            self.control.stop()
        if self.input_rabbitmq:
            self.input_rabbitmq.cancel_consumer()

    def close(self):
        logging.info("Closing queues")
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
