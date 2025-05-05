from pathlib import Path
import sys

# Utilidades de parsing

def parse_queries(filepath):
    content = Path(filepath).read_text(encoding='utf-8').strip()
    queries = {}
    current = None
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("query"):
            current = int(line.split()[1])
            queries[current] = []
        else:
            queries[current].append(line)
    return queries

# Comparaciones

def compare_query1(expected, received):
    expected_set = set(expected)
    received_set = set(received)
    missing_movies = expected_set - received_set
    extra_movies = received_set - expected_set
    return missing_movies, extra_movies

def compare_query2(expected, received):
    expected_map = {line.split(',')[0]: int(line.split(',')[1]) for line in expected}
    received_map = {line.split(',')[0]: int(line.split(',')[1]) for line in received}
    mismatches = []
    for country in expected_map:
        if country not in received_map:
            mismatches.append((country, "missing"))
            continue
        e, r = expected_map[country], received_map[country]
        if not (0.5 <= r/e <= 2):  # Se pide que tenga el mismo orden de magnitud
            mismatches.append((country, f"{e} vs {r}"))
    return mismatches

def compare_query3(expected, received):
    expected_dict = {line.split()[0].rstrip(','): (line.split(',')[1].strip(), float(line.split(',')[2])) for line in expected}
    received_dict = {line.split()[0].rstrip(','): (line.split(',')[1].strip(), float(line.split(',')[2])) for line in received}
    mismatches = []
    for label in ("max", "min"):
        if expected_dict[label] != received_dict.get(label):
            mismatches.append((label, expected_dict[label], received_dict.get(label)))
    return mismatches

def compare_query4(expected, received):
    expected_set = set(expected)
    received_set = set(received)
    missing = expected_set - received_set
    extra = received_set - expected_set
    return missing, extra

def compare_query5(expected, received):
    expected_map = {line.split()[0].upper(): float(line.split()[1]) for line in expected}
    received_map = {line.split()[0].upper(): float(line.split()[1]) for line in received}
    mismatches = []
    for feeling in expected_map:
        e, r = expected_map[feeling], received_map.get(feeling)
        if r is None or abs(e - r) > 500:  # tolerancia: diferencia absoluta razonable
            mismatches.append((feeling, e, r))
    return mismatches

# Mostrar diferencias

def pretty_print_diffs(diffs):
    for query, diff in diffs.items():
        print(f"\n==== {query.upper()} ====")
        if not diff:
            print("✅ No differences found.")
            continue

        if query == "query 1":
            expected_extra, received_extra = diff
            if expected_extra:
                print("❌ Expected but missing in received:")
                for line in sorted(expected_extra):
                    print(f"  - {line}")
            if received_extra:
                print("❌ Unexpected in received:")
                for line in sorted(received_extra):
                    print(f"  + {line}")
            if len(expected_extra) == 0 and len(received_extra) == 0:
                print("✅ No differences found.")
                continue

        elif query == "query 2":
            print("❌ Differences in magnitudes:")
            for (country, expected_val, received_val) in diff:
                print(f"  - {country}: expected ~{expected_val}, received {received_val}")

        elif query == "query 3":
            print("❌ Min/Max mismatches:")
            for label, expected_val, received_val in diff:
                print(f"  - {label}: expected {expected_val}, received {received_val}")

        elif query == "query 4":
            expected_extra, received_extra = diff
            if expected_extra:
                print("❌ Expected actors missing in received:")
                for line in sorted(expected_extra):
                    print(f"  - {line}")
            if received_extra:
                print("❌ Unexpected actors in received:")
                for line in sorted(received_extra):
                    print(f"  + {line}")
            if len(expected_extra) == 0 and len(received_extra) == 0:
                print("✅ No differences found.")
                continue

        elif query == "query 5":
            print("❌ Float differences in sentiment ratios:")
            for label, expected_val, received_val in diff:
                print(f"  - {label}: expected {expected_val}, received {received_val}")

expected_path = sys.argv[1]
received_path = sys.argv[2]

print("Comparando resultados entre los archivos", expected_path, "y", received_path)

expected = parse_queries(expected_path)
received = parse_queries(received_path)

results = {
    "query 1": compare_query1(expected[1], received[1]),
    "query 2": compare_query2(expected[2], received[2]),
    "query 3": compare_query3(expected[3], received[3]),
    "query 4": compare_query4(expected[4], received[4]),
    "query 5": compare_query5(expected[5], received[5]),
}

pretty_print_diffs(results)
