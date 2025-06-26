import threading
import json
import os
import glob
import logging
from common.logger import init_logging
from common.middleware import Middleware
from common.packet import is_delete_packet, is_final_packet
from common.atomic_write import atomic_write

init_logging(os.getenv("LOG_LEVEL", "info"))


class LeaderQueue:
    def __init__(
        self,
        final_queue,
        output_queue,
        consumer_tag,
        cluster_size,
        output_exchange=None,
    ):
        """Initialize CloseQueue with a RabbitMQ connection and queue name."""
        self.final_queue = final_queue
        self.output_queue = output_queue
        self.consumer_tag = consumer_tag
        self.cluster_size = cluster_size
        self.client_counters = {}  # dict[client_id, dict[node_id, count]]
        self.delete_list = {}

        self.final_rabbitmq = Middleware(
            queue=final_queue, consumer_tag=consumer_tag, publish_to_exchange=False
        )

        if output_exchange:
            self.output_rabbitmq = Middleware(
                queue=None,
                consumer_tag=consumer_tag,
                exchange=output_exchange,
            )
        else:
            self.output_rabbitmq = Middleware(
                queue=output_queue, consumer_tag=consumer_tag, publish_to_exchange=False
            )

        self.running = True
        self.thread = threading.Thread(target=self.consume)
        self.thread.daemon = True
        self.thread.start()

    def callback(self, ch, method, properties, body):
        """Callback to process messages; acknowledges non-final packets."""
        try:
            if self.running == False:
                self.final_rabbitmq.close_graceful(method)
                return

            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
            node_id = packet.get("node_id")
            count: int = packet.get("count", 0)

            if is_delete_packet(header):
                logging.info("Received DELETE packet for client %s", client_id)
                self.delete_list[client_id] = True
                if len(self.client_counters[client_id].keys()) == self.cluster_size:
                    self.delete_client(client_id)
                    self.output_rabbitmq.delete_queue()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            # For each client_id, keep a dict that contains the ids of the nodes
            # that sent a FINAL packet, and the count for each node
            if client_id not in self.client_counters:
                self.client_counters[client_id] = {}

            # Add the node id only if it is not already in the list
            # If a duplicate final is received, it will be ignored
            if node_id not in self.client_counters[client_id]:
                self.client_counters[client_id][node_id] = count
                # Save the state
                self.save_state(client_id)
            else:
                logging.debug(
                    "Duplicate FINAL from node: %s and client: %s", node_id, client_id
                )

            if is_final_packet(header):
                if client_id in self.delete_list:
                    logging.info(
                        "Received FINAL packet from dead client %s, ignoring it...",
                        client_id,
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                logging.info(
                    "Received FINAL packet for client %s from node %s with count %s",
                    client_id,
                    node_id,
                    count,
                )
                # If the length of the dict is equal to the cluster size, send the final
                if len(self.client_counters[client_id].keys()) == self.cluster_size:
                    total_count = 0
                    for count in self.client_counters[client_id].values():
                        total_count += count
                    self.output_rabbitmq.send_final(
                        client_id=client_id,
                        routing_key=str(client_id),
                        count=total_count,
                    )
                    logging.debug(
                        "Sent final for client %s with total_count = %s",
                        client_id,
                        total_count,
                    )
                    # Send ACK and only then delete the client data
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.delete_client(client_id)
                    return
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.warning("Error in shared callback for %s: %s", self.final_queue, e)
            ch.basic_nack(
                delivery_tag=method.delivery_tag, multiple=False, requeue=False
            )

    def consume(self):
        """Consume messages from the queue in a loop until stopped."""
        self.load_state()
        try:
            self.final_rabbitmq.consume(self.callback)

        except Exception as e:
            logging.error("Error consuming queue %s: %s", self.final_queue, e)
        finally:
            logging.info("Stopped consuming queue %s", self.final_queue)
            self.output_rabbitmq.close()
            self.final_rabbitmq.close()

    def save_state(self, client_id):
        """
        Saves the state (the dictionary that contains the finals received for each client)
        to the disk.
        """
        file = self.filename_for_client(client_id)
        content = json.dumps(self.client_counters[client_id])
        atomic_write(file, content)

    def delete_client(self, client_id):
        """
        Deletes the client state both from memory and disk
        """
        del self.client_counters[client_id]
        file = self.filename_for_client(client_id)
        try:
            os.remove(file)
            logging.info("Removed file %s for client %s", file, client_id)
        except Exception as e:
            logging.warning("Failed to remove file %s. Error: %s", file, e)

    def load_state(self):
        """
        Loads any previous state from the 'final.client_id.json' files.
        """
        # Get a list of files that match the pattern client.*.json
        state_files: list[str] = glob.glob("final.*.json")
        logging.info("Found final state files: %s", state_files)
        for file in state_files:
            client_id = int(file.split(".")[1])
            try:
                with open(file, "r", encoding="utf-8") as f:
                    state = json.loads(f.read())
                    self.client_counters[client_id] = state
            except Exception as e:
                logging.warning("Failed to read file %s. Error: %s", file, e)
            logging.debug(
                "Recovered state from client %s, state = %s", client_id, state
            )

    def filename_for_client(self, client_id) -> str:
        """
        Returns the name of the file that keeps the state for the given client
        """
        return f"final.{client_id}.json"

    def close(self):
        """Signal the thread to stop and wait for it to finish."""
        logging.info("Closing queues")
        self.running = False
        self.final_rabbitmq.cancel_consumer()
        self.join()
        self.output_rabbitmq.close()
        self.final_rabbitmq.close()

    def join(self):
        """Wait for the consumer thread to finish."""
        if self.thread.is_alive():
            self.thread.join()
