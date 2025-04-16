# filter.py
import json
import math
from common.middleware import Middleware
from common.packet import MoviePacket, handle_final_packet, is_final_packet
from datetime import datetime
import os

class FilterNode:
    def __init__(self):
        self.filters = {}
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "movie_queue")
        self.exchange = os.getenv("RABBITMQ_EXCHANGE", "")
        self.consumer_tag = os.getenv("RABBITMQ_CONSUMER_TAG", "default_consumer")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "default_output")
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE", "") 
        
        if self.output_exchange: 
            self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        else:
            self.output_rabbitmq = Middleware(queue=self.output_queue)

        if self.exchange:  # <- si hay exchange, lo usamos
            self.input_rabbitmq = Middleware(
                queue=self.input_queue,
                consumer_tag=self.consumer_tag,
                exchange=self.exchange,
                publish_to_exchange=False
            )
        else:  # <- si no, conectamos directo a la cola
            self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
       

    def check_condition(self, value, condition):
        #print(f" [DEBUG] Checking condition: value={value}, condition={condition}")
        if value is None:
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        if condition is None:
            #print(f" [DEBUG] Condition is None, returning True")
            return True
        op, target, _ = condition
        #print(f" [DEBUG] Operation: {op}, Target: {target}")
        if op == 'equal':
            result = str(value).lower() == str(target).lower()
            #print(f" [DEBUG] Equal comparison: '{str(value).lower()}' == '{str(target).lower()}' -> {result}")
            return result
        elif op == 'less':
            try:
                if value:
                    year_str = value.split('-')[0]
                    year = datetime.strptime(year_str, '%Y').year
                    result = year < float(target)
                    #print(f" [DEBUG] Less comparison: year={year}, target={float(target)} -> {result}")
                    return result
                #print(f" [DEBUG] Value is None for 'less', returning False")
                return False
            except (ValueError, TypeError) as e:
                #print(f" [DEBUG] Error in 'less' comparison: {e}")
                return False
        elif op == 'more':
            try:
                if value:
                    year_str = value.split('-')[0]
                    year = datetime.strptime(year_str, '%Y').year
                    result = year > float(target)
                    #print(f" [DEBUG] More comparison: year={year}, target={float(target)} -> {result}")
                    return result
                #print(f" [DEBUG] Value is None for 'more', returning False")
                return False
            except (ValueError, TypeError) as e:
                #print(f" [DEBUG] Error in 'more' comparison: {e}")
                return False
        elif op == 'in':
            target_lower = [str(t).lower() for t in target]
            match_found = False
            if isinstance(value, str):
                if any(t_lower in str(value).lower() for t_lower in target_lower):
                    match_found = True
            #print(f" [DEBUG] In comparison (flexible): value={value}, target={target}, result={match_found}")
            return match_found
        #print(f" [DEBUG] Unknown operation: {op}, returning False")
        return False

    def callback(self, ch, method, properties, body):
        try:
            packet_json = body.decode()
            
            if is_final_packet(json.loads(packet_json).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    self.output_rabbitmq.send_final()
                    self.input_rabbitmq.send_ack_and_close(method)
                return
            
            packet = MoviePacket.from_json(packet_json)
            movie = packet.movie
            
            # Aplicar los filtros de la instancia

            for _, condition in self.filters.items():
                _, _, key = condition
                value = movie.get(key)
                if not self.check_condition(value, condition):
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            
            filtered_packet = MoviePacket(
                #packet_id=packet.packet_id,
                timestamp=datetime.utcnow().isoformat(),
                data={"source": "filter_node"},
                movie=movie
            )

            # Publicar el paquete filtrado a la cola del gateway
           
            self.output_rabbitmq.publish(filtered_packet.to_json())
            
            print(f" [✓] Filtered and Published to {self.output_queue}: Title: {movie.get('title', 'Unknown')}, Genres: {movie.get('genres')}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [x] Message {method.delivery_tag} acknowledged")

        except json.JSONDecodeError as e:
            print(f" [!] Error decoding JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
        except Exception as e:
            print(f" [!] Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)

    def start_node(self, filters):
        self.filters = filters
        print(f" [~] Applying filters: {self.filters}")

        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in filter node: {e}")
        finally:
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()
