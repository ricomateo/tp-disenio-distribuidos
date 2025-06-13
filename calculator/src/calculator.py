import json
import threading
import os
import signal
import glob
from datetime import datetime
from src.calculation import Calculation
from common.leader_queue import LeaderQueue
from common.middleware import Middleware
from common.packet import DataPacket, is_final_packet
from common.atomic_write import atomic_write
from common.worker_protocol import WorkerProtocol

class CalculatorNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.node_id = os.getenv("NODE_ID")
        self.cluster_size = int(os.getenv("CLUSTER_SIZE", ""))
        self.finished_event = threading.Event()
        base_queue = os.getenv('RABBITMQ_QUEUE', 'movie_queue_1')
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.consumer_tag = f"{os.getenv('RABBITMQ_CONSUMER_TAG', 'default_consumer')}_{self.node_id}"
        self.exchange = os.getenv("RABBITMQ_EXCHANGE")
        self.operation = os.getenv("OPERATION", "")
        self.health_server_ip = os.getenv("HEALTH_SERVER_IP", "0.0.0.0")
        self.health_server_port = int(os.getenv("HEALTH_SERVER_PORT", "10000"))
        self.output_rabbitmq = Middleware(queue=self.output_queue)
        self.input_queue = f"{base_queue}_{self.node_id}" if self.exchange else base_queue
        self.routing_key = os.getenv("ROUTING_KEY") or self.node_id
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE")
        self.node_id_duplicate: bool = os.getenv("NODE_ID_DUPLICATE", "") == "true"
        self.calculator = Calculation(self.operation, self.exchange)
        self.final_rabbitmq = None
        self.threads = []
        self.processed_messages_by_client = {}
        
        self.leader_queue = None
        if int(self.node_id) == 0 and self.exchange != "router_negative_sentiment":
            self.leader_queue = LeaderQueue(self.final_queue, self.output_queue, self.consumer_tag, self.cluster_size)
        
        if self.final_queue:
            self.final_rabbitmq = Middleware(
            queue=self.final_queue,
            consumer_tag=self.consumer_tag,
            publish_to_exchange=False
        )
        
        if self.exchange:  # <- si hay exchange, lo usamos
            self.input_rabbitmq = Middleware(
                queue=self.input_queue,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False,
                routing_key=self.routing_key
            )
        else:  # <- si no, conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
            
        self.control = WorkerProtocol(self.health_server_ip, self.health_server_port, self.health_server_port)


    def callback(self, ch, method, properties, body):
        try:
            if not self.running:
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibo el paquete y en caso de ser el ultimo, mando los datos y el final packet
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            if header and is_final_packet(header):
                client_id = packet.get("client_id")
                results = self.calculator.get_result(client_id)
                self.output_rabbitmq.confirm_delivery()

                count = 0
                for result in results:
                    print("Resultados del cálculo:", result)
                    id = str(hash( str(self.node_id) + str(result)))
                    data_packet = DataPacket(
                        client_id=client_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={
                            "source": f"calculator_{self.operation}",
                            **result
                        },
                        id=id
                    )
                    self.output_rabbitmq.publish(data_packet.to_json())
                    count += 1

                # The node ids are duplicate in the ratio feelings calculators
                # (we have calculator_ratio_feelings_negative_0 and calculator_ratio_feelings_positive_0
                # both with node_id = 0) so to distinguish them when sending the final message,
                # we set a different node_id for the negative calculators (appending zeroes)
                if self.node_id_duplicate is True:
                    node_id = self.node_id + "0000"
                else:
                    node_id = self.node_id
                self.final_rabbitmq.send_final_with_node_id(
                    client_id=client_id, count=count, node_id=node_id
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.delete_client_data(client_id)
                return

            packet = DataPacket.from_json(packet_json)
            movie = packet.data
            client_id = packet.client_id
            id = packet.id

            # Initialize processed messages set
            if client_id not in self.processed_messages_by_client:
                self.processed_messages_by_client[client_id] = set()

            # If the message has been already processed, skip it
            if id in self.processed_messages_by_client[client_id]:
                title = movie.get("title")
                print(f"Duplicate message: id: {id}, title: {title}, client_id: {client_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Process movie using calculator
            success = self.calculator.process_movie(client_id, movie)
            # Add the packet id to the processed messages set
            self.processed_messages_by_client[client_id].add(id)

            if success:
                print(f"[client - {client_id}] Processed movie: {movie.get('id', 'Unknown')}")
                self.save_state(client_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f" [x] Message {method.delivery_tag} acknowledged")
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)


        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}, raw packet is {packet_json}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def start_node(self):
        """
        Starts the node, loading any previous state (if available)
        """
        self.load_state()
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in calculator node: {e}")
        finally:
            if self.leader_queue:
                self.leader_queue.join()
            self.close()

    def save_state(self, client_id):
        """
        Saves the state by writing (atomically) it to the hard drive.
        """
        filename = f"client.{client_id}.json"
        data = json.dumps({
            "result": self.calculator.get_raw_result(client_id),
            "processed_messages": list(self.processed_messages_by_client.get(client_id, []))
        })
        # Save the state (atomically) to a file
        atomic_write(filename, data)


    def load_state(self):
        """
        Loads the state (partial result and processed messages) from disk, if available.
        """
        # Get a list of files that match the pattern client.*.json
        state_files: list[str] = glob.glob("client.*.json")
        print(f"StateFiles = {state_files}")
        for file in state_files:
            client_id = int(file.split(".")[1])
            with open(file, "r", encoding="utf-8") as f:
                state = json.loads(f.read())
                result = state.get("result")
                self.calculator.load_result(client_id, result)
                self.processed_messages_by_client[client_id] = set(state.get("processed_messages", []))
            print(f"Recovered state from client {client_id}, result = {result}, len(processed_messages) = {len(self.processed_messages_by_client[client_id])}")

    def delete_client_data(self, client_id: int):
        """
        Deletes the client data, both from memory and disk.
        """
        self.calculator.delete_client_data(client_id)
        if client_id in self.processed_messages_by_client:
            del self.processed_messages_by_client[client_id]
        try:
            os.remove(f"client.{client_id}.json")
        except Exception as e:
            print(f"Failed to remove file for client {client_id}. Error: {e}")

    def _sigterm_handler(self, signum, _):
        print(f"Received SIGTERM signal")
        self.running = False
        if self.control:
            self.control.stop()
        if self.final_rabbitmq:
            self.final_rabbitmq.cancel_consumer()
        if self.input_rabbitmq:
            self.input_rabbitmq.cancel_consumer()
        if self.leader_queue:
            self.leader_queue.close()

    def close(self):
        print(f"Closing queues")
        if self.leader_queue:
            self.leader_queue.close()
        if self.input_rabbitmq:
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
        
       