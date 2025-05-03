import ast
import json
import os
from datetime import datetime
import threading
import signal
from common.leader_queue import LeaderQueue
from common.packet import DataPacket, QueryPacket, handle_final_packet, is_final_packet
from common.middleware import Middleware


class DeliverNode:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.running = True
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "deliver_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "query_queue")
        self.keep_columns, self.filters = self._parse_environment()
        self.consumer_tag = os.getenv('RABBITMQ_CONSUMER_TAG', 'default_consumer')
        self.output_exchange = os.getenv("RABBITMQ_OUTPUT_EXCHANGE")
        # Uso keys unicas para cada filtro basadas en la columna a sortear y la direccion del ordenamiento
        self.collected_movies = {}

        self.input_rabbitmq = Middleware(queue=self.input_queue, consumer_tag=self.consumer_tag)
        self.output_rabbitmq = Middleware(queue=None, exchange=self.output_exchange)
        
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE", "final_deliver")
        self.query_number = os.getenv("QUERY_NUMBER", "1")
        self.final_rabbitmq = Middleware(queue=self.final_queue, consumer_tag=self.consumer_tag)
        
        self.cluster_size = int(os.getenv("CLUSTER_SIZE", ""))
        self.leader_queue = None
        if int(self.query_number) == 5:
            self.leader_queue = LeaderQueue(self.final_queue, "", self.consumer_tag, self.cluster_size, output_exchange=self.output_exchange)

    def _initialize_client_movies(self, client_id):
        """Initialize movie collection for a new client."""
        if client_id not in self.collected_movies:
                # Same structure as before, but per client
                self.collected_movies[client_id] = {"default": []} if not self.filters else {
                    f"{f['column']}_{'desc' if f['inverse_sort'] else 'asc'}": []
                    for f in self.filters
                }
                
    def _parse_environment(self):
        """Parse environment variables for KEEP_COLUMNS and SORT."""

        # Parseo KEEP_COLUMNS
        keep_columns = [col.strip() for col in os.getenv("KEEP_COLUMNS", "").split(",") if col.strip()]
        
        # Parseo SORT (por ejemplo, "revenue:5,title:-1"), si no hay valor no se ordena
        sort_spec = os.getenv("SORT", "").strip()
        filters = []
        if sort_spec:
            try:
                for spec in sort_spec.split(","):
                    spec = spec.strip()
                    if ":" not in spec:
                        raise ValueError(f"Invalid SORT format: {spec}")
                    column, top_n_str = spec.split(":", 1)
                    column = column.strip()
                    top_n = int(top_n_str)
                    if not column:
                        raise ValueError(f"Empty column in SORT: {spec}")
                    top_n_val = abs(top_n) if top_n != 0 else None
                    inverse_sort = top_n < 0
                    filters.append({
                        "column": column,
                        "top_n": top_n_val,
                        "inverse_sort": inverse_sort
                    })
            except (ValueError, TypeError) as e:
                print(f" [~] Invalid SORT format: {sort_spec}, error: {e}. Storing all movies.")
                filters = []
        
        return keep_columns, filters

    def _get_sort_key(self, movie, column):
        """Generate sort key for a movie based on the column."""
        value = movie.get(column, "")
        try:
            return float(value) if value else float('-inf')
        except (ValueError, TypeError):
            return str(value).lower() if value else ""

    def _insert_sorted_movie(self, movie, column, top_n, inverse_sort, client_id):
        """Insert a movie into the sorted list for a column and trim if needed."""
        # Aca uso las keys unicas basadas en la columna y la direccion de ordenamiento
        list_key = f"{column}_{'desc' if inverse_sort else 'asc'}"
        new_key = self._get_sort_key(movie, column)
        insert_pos = 0
        is_numeric = column != "title"
        movie_list = self.collected_movies[client_id][list_key]
        for i, existing_movie in enumerate(movie_list):
                existing_key = self._get_sort_key(existing_movie, column)
                if is_numeric:
                    if (inverse_sort and new_key < existing_key) or \
                       (not inverse_sort and new_key > existing_key):
                        break
                else:
                    if (inverse_sort and new_key > existing_key) or \
                       (not inverse_sort and new_key < existing_key):
                        break
                insert_pos = i + 1
            
        movie_list.insert(insert_pos, movie)
        if top_n is not None and len(movie_list) > top_n:
                self.collected_movies[client_id][list_key] = movie_list[:top_n]

    def _process_movie(self, movie, client_id):
        """Process a movie for a specific client."""
        self._initialize_client_movies(client_id)
        
      
        if not self.filters:
            self.collected_movies[client_id]["default"].append(movie)
        else:
            for filter_spec in self.filters:
                self._insert_sorted_movie(
                    movie,
                    filter_spec["column"],
                    filter_spec["top_n"],
                    filter_spec["inverse_sort"],
                    client_id
                )
        
        return movie

    

    def _format_movie(self, movie):
        """Format a movie into a string based on KEEP_COLUMNS."""
        campos = []
        for key in self.keep_columns:
            value = movie.get(key, "")

            # Intentamos parsear strings que parezcan listas/dicts
            if isinstance(value, str):
                try:
                    parsed = ast.literal_eval(value)
                    value = parsed
                except (ValueError, SyntaxError):
                    pass

            # Ahora evaluamos el tipo
            if isinstance(value, dict):
                value = value.get("name", "")
            elif isinstance(value, list):
                value = ", ".join(
                    v.get("name", "") if isinstance(v, dict) else str(v)
                    for v in value
                )

            campos.append(f"{key}: {value}")
        return " | ".join(campos) 
        
    def _generate_response(self, client_id):
        """Generate the response string for the final packet."""
        if client_id not in self.collected_movies:
            return "No se encontraron resultados."
        lines = []
        if self.filters:
            for filter_spec in self.filters:
                column = filter_spec["column"]
                top_n = filter_spec.get("top_n")
                inverse_sort = filter_spec.get("inverse_sort")
                # Uso una key unica para ordenar la lista
                list_key = f"{column}_{'desc' if inverse_sort else 'asc'}"
                movies = self.collected_movies[client_id][list_key][:top_n] if top_n is not None else self.collected_movies[client_id][list_key]
                
                sort_dir = "ascending" if (inverse_sort != (column == "title")) else "descending"
                header = f"Top {top_n or 'all'} by {column} ({sort_dir}):"
                lines.append(header)
                
                for movie in movies:
                    lines.append(self._format_movie(movie))
                
                if movies:
                    lines.append("")
        else:
            # Caso base (sin filtros)
            movies = self.collected_movies[client_id]["default"]
            for movie in movies:
                lines.append(self._format_movie(movie))
        
        return "\n".join(lines).rstrip() if lines else "No se encontraron resultados."
    
    def callback(self, ch, method, properties, body):
        try:
            if self.running == False:
                self.input_rabbitmq.close_graceful(method)
                return
            # Recibo el paquete, si es el último mando los resultados
            body_decoded = body.decode()
            packet = json.loads(body_decoded)
            if is_final_packet(packet.get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    client_id = packet.get("client_id")
                    response_str = self._generate_response(client_id)
                    query_packet = QueryPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        response=response_str
                    )
                    self.output_rabbitmq.publish(query_packet.to_json(), str(client_id))
                    self.final_rabbitmq.send_final(int(client_id))
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            packet = DataPacket.from_json(body_decoded)
            filtered_movie = self._process_movie(packet.data, packet.client_id)

            print(f" [DeliverNode] Movie added: {filtered_movie}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [DeliverNode] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _log_startup(self):
        """Log startup information about queues, filters, and columns."""
        print(f" [~] DeliverNode listening on {self.input_queue}, will send to {self.output_queue}")
        if self.keep_columns:
            print(f" [~] Filtering movie fields: {self.keep_columns}")
        if self.filters:
            for filter_spec in self.filters:
                column = filter_spec["column"]
                top_n = filter_spec.get("top_n")
                inverse_sort = filter_spec.get("inverse_sort")
                sort_dir = "ascending" if (inverse_sort != (column == "title")) else "descending"
                top_n_str = f"top {top_n}" if top_n is not None else "all"
                print(f" [~] Sorting by {column} ({sort_dir}, {top_n_str})")
        else:
            print(f" [~] No sorting, storing movies as they arrive")

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
        print(f"Received SIGTERM signal")
        self.close()
    
    def close(self):
        print(f"Closing queues")
        self.running = False
        if self.leader_queue:
            self.leader_queue.close()
        if self.input_rabbitmq:
            self.input_rabbitmq.cancel_consumer()
            self.input_rabbitmq.close()
        if self.output_rabbitmq:
            self.output_rabbitmq.close()
        if self.final_rabbitmq:
            self.final_rabbitmq.close()