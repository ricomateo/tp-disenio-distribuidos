# check_condition.py
import ast
import math
from datetime import datetime

def parse_string(value):
    try:
        # Parseo el string JSON a un objeto de Python
        parsed = ast.literal_eval(value)
        return parsed
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing string: {e}")
        return None

def check_condition(value, condition):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return False
    if condition is None:
        return True
    op, target, _ = condition
    if op == 'more_date':
        try:
            if value:
                year_str = value.split('-')[0]
                year = datetime.strptime(year_str, '%Y').year
                result = year > float(target)
                return result
            return False
        except (ValueError, TypeError) as e:
            return False
    elif op == 'less_date':
        try:
            if value:
                year_str = value.split('-')[0]
                year = datetime.strptime(year_str, '%Y').year
                result = year < float(target)
                return result
            return False
        except (ValueError, TypeError) as e:
            return False
    elif op == 'more':
        try:
            if value:
                num = int(value)  
                result = num > float(target)
                return result
            return False
        except (ValueError, TypeError) as e:
            return False
    elif op == 'less':
        try:
            if value:
                num = int(value)  
                result = num < float(target)
                return result
            return False
        except (ValueError, TypeError) as e:
            return False
    elif op == 'equal':
        result = str(value).lower() == str(target).lower()
        return result
    elif op == 'in':
        parsed = parse_string(value) if isinstance(value, str) else value
        target_lower = [str(t).lower() for t in target]
        match_found = False
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    # Si es un diccionario compara contra todos sus elementos
                    for key, item_value in item.items():
                        if any(t_lower == str(item_value).lower() for t_lower in target_lower):
                            match_found = True
                            break
                    if match_found:
                        break
                elif any(t_lower == str(item).lower() for t_lower in target_lower):
                    match_found = True
                    break
        elif isinstance(parsed, str):
            match_found = any(t_lower in str(parsed).lower() for t_lower in target_lower)
        return match_found
    elif op == "count":
        parsed = parse_string(value) if isinstance(value, str) else value
        if not isinstance(parsed, list):
            return False
        return len(parsed) == int(target)
        
    return False

   