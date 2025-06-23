import threading
import json
import os
import glob
import sys
from common.middleware import Middleware
from common.packet import is_delete_packet, is_final_packet
from common.atomic_write import atomic_write

class LeaderQueue:
    def __init__(self, final_queue, output_queue, consumer_tag, cluster_size, output_exchange = None):
        """Initialize CloseQueue with a RabbitMQ connection and queue name."""
        self.final_queue = final_queue
        self.output_queue = output_queue
        self.consumer_tag = consumer_tag
        self.cluster_size = cluster_size
        self.client_counters = {} # dict[client_id, dict[node_id, count]]
        self.delete_list = []
        
        self.final_rabbitmq = Middleware(
            queue=final_queue,
            consumer_tag=consumer_tag,
            publish_to_exchange=False
        )

        if output_exchange:
            self.output_rabbitmq = Middleware(
                queue=None,
                consumer_tag=consumer_tag,
                exchange=output_exchange,
            )
        else:
            self.output_rabbitmq = Middleware(
                queue=output_queue,
                consumer_tag=consumer_tag,
                publish_to_exchange=False
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
            print(f"node_id = {node_id}")

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
            else: # TODO: remove this, only for debugging
                print(f"Duplicate final from node: {node_id}")
                
            if is_delete_packet(header):
                self.delete_list[client_id] = True
                if len(self.client_counters[client_id].keys()) == self.cluster_size:
                    self.delete_client(client_id)
                    self.output_rabbitmq.delete_queue()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            if is_final_packet(header):
                # If the length of the dict is equal to the cluster size, send the final
                if len(self.client_counters[client_id].keys()) == self.cluster_size:
                    total_count = 0
                    for count in self.client_counters[client_id].values():
                        total_count += count
                    print(f"Sending final with total_count = {total_count}")
                    self.output_rabbitmq.send_final(
                        client_id=client_id, routing_key=str(client_id), count=total_count
                    )
                    # Send ACK and only then delete the client data
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    if client_id not in self.delete_list:
                        self.delete_client(client_id)
                    return
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [!] Error in shared callback for {self.final_queue}: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def consume(self):
        """Consume messages from the queue in a loop until stopped."""
        self.load_state()
        try:
            self.final_rabbitmq.consume(self.callback)

        except Exception as e:
            print(f" [!] Error consuming queue {self.final_queue}: {e}")
        finally:
            print(f" [!] Stopped consuming queue {self.final_queue}")
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
        except Exception as e:
            print(f"Failed to remove file {file}. Error: {e}")

    def load_state(self):
        """
        Loads any previous state from the 'final.client_id.json' files.
        """
        # Get a list of files that match the pattern client.*.json
        state_files: list[str] = glob.glob("final.*.json")
        print(f"FinalFiles = {state_files}")
        for file in state_files:
            client_id = int(file.split(".")[1])
            try:
                with open(file, "r", encoding="utf-8") as f:
                    state = json.loads(f.read())
                    self.client_counters[client_id] = state
            except Exception as e:
                print(f"Failed to read file {file}. Error: {e}")
            print(f"Recovered state from client {client_id}, state = {state}")


    def filename_for_client(self, client_id) -> str:
        """
        Returns the name of the file that keeps the state for the given client
        """
        return f"final.{client_id}.json"

    def close(self):
        """Signal the thread to stop and wait for it to finish."""
        self.running = False
        self.final_rabbitmq.cancel_consumer()
        self.join()
        self.output_rabbitmq.close()
        self.final_rabbitmq.close()

    def join(self):
        """Wait for the consumer thread to finish."""
        if self.thread.is_alive():
            self.thread.join()
