# check_condition.py
import ast
import math
from datetime import datetime

def parse_string(value):
    try:
        # Parse the JSON string into a Python object
        parsed = ast.literal_eval(value)
        #print(f"Parsed result: {parsed!r}")
        return parsed
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing string: {e}")
        return None

def check_condition(value, condition):
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
            parsed = parse_string(value) if isinstance(value, str) else value
            target_lower = [str(t).lower() for t in target]
            match_found = False
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        # Check all values in the dictionary
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
                # Fallback for unparsed strings
                match_found = any(t_lower in str(parsed).lower() for t_lower in target_lower)
            return match_found
        elif op == "count":
            parsed = parse_string(value) if isinstance(value, str) else value
            if not isinstance(parsed, list):
                return False
            return len(parsed) == int(target)
                
        #print(f" [DEBUG] Unknown operation: {op}, returning False")
        return False

   