import threading
import json
from common.middleware import Middleware
from common.packet import DataPacket, handle_final_packet, is_final_packet

class LeaderQueue:
    def __init__(self, final_queue, output_queue, consumer_tag, cluster_size, output_exchange = None):
        """Initialize CloseQueue with a RabbitMQ connection and queue name."""
        self.final_queue = final_queue
        self.output_queue = output_queue
        self.consumer_tag = consumer_tag
        self.cluster_size = cluster_size
        self.client_counters = {}
        self.counter = 0
        
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
        self.thread
        self.thread.daemon = True  
        self.thread.start()
            
    def callback(self, ch, method, properties, body):
        """Callback to process messages; acknowledges non-final packets."""
        try:
            packet_json = body.decode()
            packet = json.loads(packet_json)
            header = packet.get("header")
            client_id = packet.get("client_id")
           
            self.client_counters[client_id] = self.client_counters.get(client_id, 0) + 1
            
            if is_final_packet(header):
                self.counter += 1
                if self.client_counters[client_id] == self.cluster_size:
                    self.output_rabbitmq.send_final(client_id=client_id, routing_key=str(client_id))
                    del self.client_counters[client_id]
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [!] Error in shared callback for {self.final_queue}: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    def consume(self):
        """Consume messages from the queue in a loop until stopped."""

        try:
            self.final_rabbitmq.consume(self.callback)
           
        except Exception as e:
            print(f" [!] Error consuming queue {self.final_queue}: {e}")
        finally:
            print(f" [!] Stopped consuming queue {self.final_queue}")
            
    def close(self):
        """Stop consuming and close the queue."""
        print(f"Closing queue {self.final_queue}")
        self.running = False
        self.final_rabbitmq.cancel_consumer()
        self.output_rabbitmq.close()
        self.final_rabbitmq.close()

    def join(self):
        """Wait for the consumer thread to finish."""
        if self.thread.is_alive():
            self.thread.join()
        print(f" [!] Thread for queue {self.final_queue} joined")