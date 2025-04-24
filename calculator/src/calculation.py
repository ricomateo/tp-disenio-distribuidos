import ast
import json
from typing import Dict, Tuple, List, Set

COUNT = "COUNT_BY"
AVERAGE = "AVERAGE_BY"
RATIO = "RATIO_BY"
SUM = "SUM_BY"

class Calculation:
    def __init__(self, operation: str, input_queue: str):
        self.operation = operation
        self.input_queue = input_queue
        # Parse operation string
        try:
            op_parts = operation.split(":", 1)
            if len(op_parts) != 2:
                raise ValueError("Operation must contain exactly one ':'")
            op_type, args = op_parts
            self.op_type = op_type.upper()
            if self.op_type == COUNT:
                self.key = args
                self.counts: Dict[str, int] = {}  # key_value -> count
            elif self.op_type == AVERAGE:
                self.key, self.value_field = args.split(",", 1)
                self.averages: Dict[str, Tuple[float, int]] = {}  # key_value -> (total, count)
            elif self.op_type == RATIO:
                self.numerator, self.denominator = args.split(",", 1)
                self.totals: Tuple[float, float, int] = (0.0, 0.0, 0)  # (total_numerator, total_denominator, count)
            elif self.op_type == SUM:
                self.key, self.value_field = args.split(",", 1)
                self.sums: Dict[str, float] = {}  # key_value -> sum
            else:
                raise ValueError(f"Unknown operation type: {op_type}")
        except Exception as e:
            raise ValueError(f"Invalid OPERATION format '{operation}': {e}")

    def parse_json_string(self, value):
        """Parse a JSON or Python literal string into a Python object."""
        if not isinstance(value, str):
            return value
        value = value.strip()
        if not value:
            return value
        try:
            parsed = ast.literal_eval(value)
            #print(f"Parsed result: {parsed!r}")
            return parsed
        except (ValueError, SyntaxError) as e:
            print(f"Error parsing string '{value}': {e}")
        try:
            parsed = json.loads(value)
            print(f"Parsed result: {parsed!r}")
            return parsed
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON string '{value}': {e}")
        return None
    
    def process_count_operation(self, movie: Dict) -> bool:
        title = movie.get("title", "Unknown")

        keys = movie.get(self.key, [])

        # Manejar las keys como una lista o un solo valor
        parsed_keys = self.parse_json_string(keys) if isinstance(keys, str) else keys
        if not isinstance(parsed_keys, list):
            parsed_keys = [parsed_keys] if parsed_keys is not None else []

        processed = False
        for key_item in parsed_keys:
            if isinstance(key_item, dict):
                # Si es un conjunto, combino todos los valores en una sola key
                key_value = key_item.get('name', '')
                if key_value:
                    combined_key = key_value
                    self.counts[combined_key] = self.counts.get(combined_key, 0) + 1
                    processed = True
            elif isinstance(key_item, str):
                # Si es un solo valor, lo sumo a la cuenta
                if key_item:
                    self.counts[key_item] = self.counts.get(key_item, 0) + 1
                    processed = True

        if not processed:
            print(f"Skipped movie '{title}' with missing or invalid {self.key}")

        return processed

    def process_average_operation(self, movie: Dict) -> bool:
        title = movie.get("title", "Unknown")

        keys = movie.get(self.key, [])
        value = movie.get(self.value_field)
        
        # Manejar las keys como una lista o un solo valor
        parsed_keys = self.parse_json_string(keys) if isinstance(keys, str) else keys
        if not isinstance(parsed_keys, list):
            parsed_keys = [parsed_keys] if parsed_keys is not None else []
        if value is not None:
            try:
                value = float(value)
                processed = False

                for key_item in parsed_keys:
                    if isinstance(key_item, dict):
                        # Si es un diccionario junto los valores no nulos en una key
                        key_value = key_item.get('name', '')
                        if key_value:
                            combined_key = key_value
                            current_total, current_count = self.averages.get(combined_key, (0.0, 0))
                            self.averages[combined_key] = (current_total + value, current_count + 1)
                            processed = True

                    elif isinstance(key_item, str) and key_item:
                        # Si es una key agrego el valor al promedio
                        if key_item:
                            current_total, current_count = self.averages.get(key_item, (0.0, 0))
                            self.averages[key_item] = (current_total + value, current_count + 1)
                            processed = True

                    elif isinstance(key_item, (int, float)):
                        # Si la key no es un str, la convierto y después promedio el valor
                        key_str = str(key_item)
                        current_total, current_count = self.averages.get(key_str, (0.0, 0))
                        self.averages[key_str] = (current_total + value, current_count + 1)
                        processed = True

                if not processed:
                    print(f"Skipped movie '{title}' with invalid {self.key}")
                return processed
            except (ValueError, TypeError):
                print(f"Skipped movie '{title}' with invalid {self.value_field}")
                return False
        return False
    
    def process_ratio_operation(self, movie: Dict) -> bool:
        title = movie.get("title", "Unknown")
    
        numerator = movie.get(self.numerator, 0)
        denominator = movie.get(self.denominator, 0)
        try:
            # Convierto los numeros a float y calculo los nuevos ratios
            numerator = float(numerator)
            denominator = float(denominator)
            if denominator == 0:
                return False

            total_numerator, total_denominator, count = self.totals
            self.totals = (total_numerator + numerator, total_denominator + denominator, count + 1)

            return True
        except (ValueError, TypeError):
            print(f"Skipped movie '{title}' with invalid {self.numerator} or {self.denominator}")
            return False

    def process_sum_operation(self, movie: Dict) -> bool:
        title = movie.get("title", "Unknown")

        value = movie.get(self.value_field, 0)
        keys = movie.get(self.key, [])

        try:
            value = int(value)
            if value == 0:
                return False

            parsed_keys = self.parse_json_string(keys) if isinstance(keys, str) else keys
            if not isinstance(parsed_keys, list):
                print(f"Skipped movie '{title}' with invalid {self.key}")
                return False

            for key_item in parsed_keys:
                if isinstance(key_item, dict):
                    key_value = key_item.get('name', '')

                    if key_value:
                        combined_key = key_value
                        self.sums[combined_key] = self.sums.get(combined_key, 0) + value
                elif isinstance(key_item, str):
                    if key_item:
                        self.sums[key_item] = self.sums.get(key_item, 0) + value
            return True
        except (ValueError, TypeError):
            print(f"Skipped movie '{title}' with invalid {self.value_field}")
            return False
        
    def process_movie(self, movie: Dict) -> bool:
        """Process a movie based on the operation, return True if processed successfully."""
        try:
            title = movie.get("title", "Unknown")
           
            if self.op_type == COUNT:
                return self.process_count_operation(movie)
            elif self.op_type == AVERAGE:
                return self.process_average_operation(movie)
            elif self.op_type == RATIO:
                return self.process_ratio_operation(movie)
            elif self.op_type == SUM:
                return self.process_sum_operation(movie)
            return False

        except Exception as e:
            print(f"Error processing movie '{title}': {e}")
            return False

    def get_result(self) -> List[Dict]:
        """Return the results as a list of dictionaries, one per result item."""
        if self.op_type == COUNT:
            if not self.counts:
                return [{"error": f"No {self.key} values found."}]
            return [
                {
                    "operation": "count",
                    "key": self.key,
                    "value": key,
                    "count": count
                }
                for key, count in sorted(self.counts.items())
            ]

        elif self.op_type == AVERAGE:
            if not self.averages:
                return [{"error": f"No movies processed for {self.value_field} average by {self.key}."}]
            return [
                {
                    "operation": "average",
                    "key": self.key,
                    "id": int(float(key)),
                    "value_field": self.value_field,
                    "average": round(total / count, 2),
                    "count": count
                }
                for key, (total, count) in sorted(self.averages.items())
            ]

        elif self.op_type == RATIO:
            total_numerator, total_denominator, count = self.totals
            if count == 0 or total_denominator == 0:
                return [{"error": f"No movies processed for {self.numerator}/{self.denominator} totals."}]
            total = total_numerator / total_denominator
            feeling_str = "POS" if self.input_queue == "sentiment_positive_queue" else "NEG"
            return [
                {
                    "operation": "ratio",
                    "feeling": feeling_str,
                    "ratio": round(total, 2),
                    "count": count
                }
            ]

        elif self.op_type == SUM:
            if not self.sums:
                return [{"error": f"No sums processed for {self.value_field} by {self.key}."}]
            return [
                {
                    "operation": "sum",
                    "key": self.key,
                    "value": key,
                    "value_field": self.value_field,
                    "total": value
                }
                for key, value in sorted(self.sums.items())
            ]

        return [{"error": "No results available."}]
