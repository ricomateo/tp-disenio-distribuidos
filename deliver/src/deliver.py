import json
import os
from datetime import datetime
import threading
from common.packet import DataPacket, MoviePacket, QueryPacket, handle_final_packet, is_final_packet
from common.middleware import Middleware


class DeliverNode:
    def __init__(self):
        self.input_queue = os.getenv("RABBITMQ_QUEUE", "deliver_queue")
        self.output_queue = os.getenv("RABBITMQ_OUTPUT_QUEUE", "query_queue")
        self.keep_columns, self.filters = self._parse_environment()
        # Use unique keys for each filter based on column and sort direction
        self.collected_movies = {"default": []} if not self.filters else {
            f"{f['column']}_{'desc' if f['inverse_sort'] else 'asc'}": [] for f in self.filters
        }
        self.finished_event = threading.Event()
        self.input_rabbitmq = Middleware(queue=self.input_queue)
        self.output_rabbitmq = Middleware(queue=self.output_queue)
        
        self.final_queue = os.getenv("RABBITMQ_FINAL_QUEUE", "final_deliver")
        self.query_number = os.getenv("QUERY_NUMBER", "1")
        self.final_rabbitmq = Middleware(queue=self.final_queue)


    def _parse_environment(self):
        """Parse environment variables for KEEP_COLUMNS and SORT."""
        # Parse KEEP_COLUMNS
        keep_columns = [col.strip() for col in os.getenv("KEEP_COLUMNS", "").split(",") if col.strip()]
        
        # Parse SORT (e.g., "revenue:5,title:-1")
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

    def _insert_sorted_movie(self, movie, column, top_n, inverse_sort):
        """Insert a movie into the sorted list for a column and trim if needed."""
        # Use unique key based on column and sort direction
        list_key = f"{column}_{'desc' if inverse_sort else 'asc'}"
        new_key = self._get_sort_key(movie, column)
        insert_pos = 0
        is_numeric = column != "title"
        for i, existing_movie in enumerate(self.collected_movies[list_key]):
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
        
        self.collected_movies[list_key].insert(insert_pos, movie)
        if top_n is not None and len(self.collected_movies[list_key]) > top_n:
            self.collected_movies[list_key] = self.collected_movies[list_key][:top_n]

    def _process_movie(self, movie):
        """Process a movie by applying filters or appending to default list."""
        if not self.filters:
            self.collected_movies["default"].append(movie)
        else:
            for filter_spec in self.filters:
                self._insert_sorted_movie(
                    movie,
                    filter_spec["column"],
                    filter_spec["top_n"],
                    filter_spec["inverse_sort"]
                )
        
        return movie

    def _format_movie(self, movie):
        """Format a movie into a string based on KEEP_COLUMNS."""
        campos = []
        for key in self.keep_columns:
            value = movie.get(key, "")
            if isinstance(value, list):
                value = ", ".join(map(str, value))
            campos.append(f"{key}: {value}")
        return " | ".join(campos)

    def _generate_response(self):
        """Generate the response string for the final packet."""
        lines = []
        if self.filters:
            for filter_spec in self.filters:
                column = filter_spec["column"]
                top_n = filter_spec.get("top_n")
                inverse_sort = filter_spec.get("inverse_sort")
                # Use unique key for the list
                list_key = f"{column}_{'desc' if inverse_sort else 'asc'}"
                movies = self.collected_movies[list_key][:top_n] if top_n is not None else self.collected_movies[list_key]
                
                sort_dir = "ascending" if (inverse_sort != (column == "title")) else "descending"
                header = f"Top {top_n or 'all'} by {column} ({sort_dir}):"
                lines.append(header)
                
                for movie in movies:
                    lines.append(self._format_movie(movie))
                
                if movies:
                    lines.append("")
        else:
            # Handle the default case (no filters)
            movies = self.collected_movies["default"]
            for movie in movies:
                lines.append(self._format_movie(movie))
        
        return "\n".join(lines).rstrip() if lines else "No se encontraron películas."

    def callback(self, ch, method, properties, body):
        try:
            body_decoded = body.decode()
            
            if is_final_packet(json.loads(body_decoded).get("header")):
                if handle_final_packet(method, self.input_rabbitmq):
                    response_str = self._generate_response()
                    query_packet = QueryPacket(
                        timestamp=datetime.utcnow().isoformat(),
                        data={"source": self.input_queue},
                        response=response_str
                    )
                    self.output_rabbitmq.publish(query_packet.to_json())
                    self.input_rabbitmq.send_ack_and_close(method)
                return

            packet = DataPacket.from_json(body_decoded)
            filtered_movie = self._process_movie(packet.data)
            print(f" [DeliverNode] Movie added: {filtered_movie}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [DeliverNode] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

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
        t3 = threading.Thread(target=self.final_rabbitmq.consume, args=(self.noop_callback,))
        if int(self.query_number) == 1:
            self.final_rabbitmq.send_final()  
        t3.start()
        try:
            self.input_rabbitmq.consume(self.callback)
        except Exception as e:
            print(f" [!] Error in deliver node: {e}")
        finally:
            self.finished_event.set()
            t3.join()
            if self.input_rabbitmq:
                self.input_rabbitmq.close()
            if self.output_rabbitmq:
                self.output_rabbitmq.close()

    def noop_callback(self, ch, method, properties, body):
        packet_json = body.decode()
        header = json.loads(packet_json).get("header")
     
        self.finished_event.wait()
        
        if is_final_packet(header):
            print(f" [!] Final rabbitmq stop consuming.")
            if handle_final_packet(method, self.final_rabbitmq):
                self.output_rabbitmq.send_final()
                self.final_rabbitmq.send_ack_and_close(method)
                print(f" [!] Final rabbitmq send final.")
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)
