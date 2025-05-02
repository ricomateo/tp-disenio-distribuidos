import re

def adapt_output(text: str) -> str:
    lines = text.strip().splitlines()
    result = []
    section = None
    query_count = 1

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Query 1
        if line.startswith("title:"):
            if section != "query1":
                result.append(f"query {query_count}")
                query_count += 1
                section = "query1"
            match = re.match(r"title: (.+?) \| genres: (.+)", line)
            if match:
                title, genres = match.groups()
                genres_list = [g.strip() for g in genres.split(",")]
                result.append(f"{title},[{', '.join(repr(g) for g in genres_list)}]")

        # Query 2
        elif line.startswith("value:") and "total" in line:
            if section != "query2":
                result.append(f"query {query_count}")
                query_count += 1
                section = "query2"
            match = re.match(r"value: (.+?) \| total: (\d+)", line)
            if match:
                country, total = match.groups()
                result.append(f"{country},{total}")

        # Query 3
        elif line.startswith("Top 1 by average (descending):"):
            if section != "query3":
                result.append(f"query {query_count}")
                query_count += 1
                section = "query3"
        elif line.startswith("Top 1 by average (ascending):"):
            continue
        elif line.startswith("id:") and "average:" in line:
            match = re.match(r"id: \d+ \| title: (.+?) \| average: ([\d.]+)", line)
            if match:
                title, avg = match.groups()
                prev_line = lines[lines.index(line) - 1]
                prefix = "max" if "descending" in prev_line else "min"
                result.append(f"{prefix} {title},{avg}")

        # Query 4
        elif line.startswith("value:") and "count" in line:
            if section != "query4":
                result.append(f"query {query_count}")
                query_count += 1
                section = "query4"
            match = re.match(r"value: (.+?) \| count: (\d+)", line)
            if match:
                actor, count = match.groups()
                result.append(f"{actor},{count}")

        # Query 5
        elif line.startswith("feeling:"):
            if section != "query5":
                result.append(f"query {query_count}")
                query_count += 1
                section = "query5"
            match = re.match(r"feeling: (\w+) \| ratio: ([\d.]+)", line)
            if match:
                feeling, ratio = match.groups()
                result.append(f"{feeling.upper()}\t{ratio}")

    return "\n".join(result)

with open("output/results.txt", "r", encoding="utf-8") as infile:
    input_text = infile.read()

adapted_output = adapt_output(input_text)
with open("testing/received_output.txt", "w", encoding="utf-8") as outfile:
    outfile.write(adapted_output)

print("Terminé de convertir el output. El nuevo archivo se llevó a 'testing/received_output.txt'.")
