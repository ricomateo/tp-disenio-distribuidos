# main.py
from src.filter import FilterNode
import os

def parse_filter_argument(filter_str):
    filters = {}
    count = 0
    if filter_str:
        conditions = filter_str.split(';')
        for condition in conditions:
            count += 1
            parts = condition.strip().split(':')
            if len(parts) == 2:
                key = parts[0].strip()
                value_part = parts[1].strip()
                if value_part.startswith('equal(') and value_part.endswith(')'):
                    target = value_part[len('equal('):-1].strip()
                    filters[count] = ('equal', target, key)
                elif value_part.startswith('less(') and value_part.endswith(')'):
                    try:
                        target = float(value_part[len('less('):-1].strip())
                        filters[count] = ('less', target, key)
                    except ValueError:
                        print(f" [!] Invalid 'less' value: {value_part}")
                elif value_part.startswith('more(') and value_part.endswith(')'):
                    try:
                        target = float(value_part[len('more('):-1].strip())
                        filters[count] = ('more', target, key)
                    except ValueError:
                        print(f" [!] Invalid 'more' value: {value_part}")
                elif value_part.startswith('less_date(') and value_part.endswith(')'):
                    try:
                        target = float(value_part[len('less_date('):-1].strip())
                        filters[count] = ('less_date', target, key)
                    except ValueError:
                        print(f" [!] Invalid 'less_date' value: {value_part}")
                elif value_part.startswith('more_date(') and value_part.endswith(')'):
                    try:
                        target = float(value_part[len('more_date('):-1].strip())
                        filters[count] = ('more_date', target, key)
                    except ValueError:
                        print(f" [!] Invalid 'more_date' value: {value_part}")
                elif value_part.startswith('in(') and value_part.endswith(')'):
                    targets_str = value_part[len('in('):-1].strip()
                    targets = [t.strip() for t in targets_str.split(',')]
                    filters[count] = ('in', targets, key)
                elif value_part.startswith('count(') and value_part.endswith(')'):
                    target = float(value_part[len('count('):-1].strip())
                    filters[count] = ('count', target, key)
                else:
                    filters[count] = ('equal', value_part, key) # Default to equal if no function
            else:
                print(f" [!] Invalid filter format: {condition}")
    return filters

if __name__ == '__main__':
    node = FilterNode()
    filter_string = os.getenv("FILTERS")
    parsed_filters = parse_filter_argument(filter_string)
    print(f" [~] Applying filters: {parsed_filters}")
    node.start_node(parsed_filters)