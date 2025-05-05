import re
import sys

def adapt_output(text: str) -> str:
    lines = text.strip().splitlines()

    q1, q2, q3, q4, q5 = [], [], [], [], []
    current_section = None
    current_avg_direction = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("title:"):
            current_section = "query1"
            match = re.match(r"title: (.+?) \| genres: (.+)", line)
            if match:
                title, genres = match.groups()
                genres_list = [g.strip() for g in genres.split(",")]
                q1.append(f"{title},[{', '.join(repr(g) for g in genres_list)}]")

        elif line.startswith("Top 5 by total"):
            current_section = "query2"

        elif current_section == "query2" and line.startswith("value:") and "total" in line:
            match = re.match(r"value: (.+?) \| total: (\d+)", line)
            if match:
                country, total = match.groups()
                q2.append(f"{country},{total}")

        elif line.startswith("Top 10 by count"):
            current_section = "query4"

        elif current_section == "query4" and line.startswith("value:") and "count" in line:
            match = re.match(r"value: (.+?) \| count: (\d+)", line)
            if match:
                actor, count = match.groups()
                q4.append(f"{actor},{count}")

        elif line.startswith("Top 1 by average (descending):"):
            current_section = "query3"
            current_avg_direction = "max"

        elif line.startswith("Top 1 by average (ascending):"):
            current_section = "query3"
            current_avg_direction = "min"

        elif current_section == "query3" and line.startswith("id:") and "average:" in line:
            match = re.match(r"id: \d+ \| title: (.+?) \| average: ([\d.]+)", line)
            if match:
                title, avg = match.groups()
                q3.append(f"{current_avg_direction}, {title},{avg}")

        elif line.startswith("Top 2 by ratio"):
            current_section = "query5"

        elif current_section == "query5" and line.startswith("feeling:"):
            match = re.match(r"feeling: (\w+) \| ratio: ([\d.]+)", line)
            if match:
                feeling, ratio = match.groups()
                feeling = "POSITIVE" if feeling == "POS" else "NEGATIVE"
                q5.append(f"{feeling}\t{ratio}")

    # Armar salida final en orden
    result = []
    if q1:
        result.append("query 1")
        result.extend(q1)
        result.append("")
    if q2:
        result.append("query 2")
        result.extend(q2)
        result.append("")
    if q3:
        result.append("query 3")
        result.extend(q3)
        result.append("")
    if q4:
        result.append("query 4")
        result.extend(q4)
        result.append("")
    if q5:
        result.append("query 5")
        result.extend(q5)
        result.append("")

    return "\n".join(result)

input_name = sys.argv[1]
output_name = sys.argv[2]

with open(input_name, "r", encoding="utf-8") as infile:
    input_text = infile.read()

adapted_output = adapt_output(input_text)
with open(output_name, "w", encoding="utf-8") as outfile:
    outfile.write(adapted_output)

print(f"Terminé de convertir el output. El nuevo archivo se llevó a '{output_name}'.")
print()
