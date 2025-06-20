import subprocess
import configparser
import sys
import json


def json_file_to_dict(file: str):
    """
    Reads the given file as JSON and returns a dictionary representing the file contents.
    """
    try:
        with open(file, "r", encoding="utf-8") as f:
            data = f.read()
            return json.loads(data)
    except Exception as e:
        print(f"Failed to read from file {file}. Error: {e}")


def compare_query_1(expected_result, client_result):
    """
    Compares the expected result with the client's result, and returns the
    missing titles in the client's result.
    """

    diffs = []
    if len(expected_result) != len(client_result):
        diffs.append(
            f"Result length mismatch: Expected {len(expected_result)} elements, got {len(client_result)}."
        )
        min_len = min(len(expected_result), len(client_result))
    else:
        min_len = len(expected_result)

    expected_titles = []
    for result in expected_result:
        expected_titles.append(result.get("title"))

    for i in range(min_len):
        client_item = client_result[i]
        client_title = client_result[i].get("title")
        if client_title is None:
            diffs.append(f"Missing 'title' key at index {i}: {client_item}")
        if client_title not in expected_titles:
            diffs.append(f"Received unexpected title {client_title}")

    return diffs


def compare_query_2(expected_result, client_result):
    """
    Compares the expected result with the client's result
    """
    diffs = []
    if len(expected_result) != len(client_result):
        diffs.append(
            f"Result length mismatch: Expected {len(expected_result)} elements, got {len(client_result)}."
        )
        min_len = min(len(expected_result), len(client_result))
    else:
        min_len = len(expected_result)

    for i in range(min_len):
        expected_item = expected_result[i]
        client_item = client_result[i]

        expected_country = expected_item.get("country")
        expected_budget = expected_item.get("budget")

        client_country = client_item.get("country")
        client_budget = client_item.get("budget")

        if "country" not in client_item:
            diffs.append(f"Missing 'country' key at index {i}: {client_item}")
        if "budget" not in client_item:
            diffs.append(f"Missing 'budget' key at index {i}: {client_item}")

        if client_country != expected_country:
            diffs.append(
                f"Country mismatch at index {i}: Expected '{expected_country}', got '{client_country}'."
            )
        else:
            if expected_budget != client_budget:
                diffs.append(
                    f"Budget mismatch for '{expected_country}' at index {i}: expected budget {expected_budget}, got {client_budget}."
                )

    return diffs


def compare_query_3(expected_result, client_result):
    """
    Compares the expected result with the client's result
    """
    diffs = []
    if len(expected_result) != len(client_result):
        diffs.append(
            f"List length mismatch: Expected {len(expected_result)} elements, got {len(client_result)}."
        )
        min_len = min(len(expected_result), len(client_result))
    else:
        min_len = len(expected_result)

    for i in range(min_len):
        expected_item = expected_result[i]
        client_item = client_result[i]

        expected_title = expected_item.get("title")
        expected_rating = expected_item.get("rating")

        client_title = client_item.get("title")
        client_rating = client_item.get("rating")

        if "title" not in client_item:
            diffs.append(f"Missing 'title' key at index {i}: {client_item}")
        if "rating" not in client_item:
            diffs.append(f"Missing 'rating' key at index {i}: {client_item}")

        if client_title != expected_title:
            diffs.append(
                f"Title mismatch at index {i}: Expected '{expected_title}', got '{client_title}'."
            )
        else:
            if expected_rating != client_rating:
                diffs.append(
                    f"Rating mismatch for '{expected_title}' at index {i}: expected {expected_rating}, got {client_rating}."
                )

    return diffs


def compare_query_4(expected_result, client_result):
    """
    Compares the expected result with the client's result
    """
    diffs = []
    if len(expected_result) != len(client_result):
        diffs.append(
            f"List length mismatch: Expected {len(expected_result)} elements, got {len(client_result)}."
        )
        min_len = min(len(expected_result), len(client_result))
    else:
        min_len = len(expected_result)

    for i in range(min_len):
        expected_item = expected_result[i]
        client_item = client_result[i]

        expected_name = expected_item.get("name")
        expected_count = expected_item.get("count")

        client_name = client_item.get("name")
        client_count = client_item.get("count")

        if "name" not in client_item:
            diffs.append(f"Missing 'name' key at index {i}: {client_item}")
        if "count" not in client_item:
            diffs.append(f"Missing 'count' key at index {i}: {client_item}")

        if client_name != expected_name:
            diffs.append(
                f"Name mismatch at index {i}: Expected '{expected_name}', got '{client_name}'."
            )
        else:
            if expected_count != client_count:
                diffs.append(
                    f"Count mismatch for '{expected_name}' at index {i}: expected {expected_count}, got {client_count}."
                )

    return diffs


def compare_query_5(expected_result, client_result):
    """
    Compares the expected result with the client's result
    """
    # Sort both results so that "NEGATIVE" ratios are first.
    expected_result = sorted(expected_result, key=lambda k: k["feeling"])
    client_result = sorted(client_result, key=lambda k: k["feeling"])

    expected_len = 2
    diffs = []
    if len(client_result) != 2:
        diffs.append(
            f"List length mismatch: Expected {expected_len} elements, got {len(client_result)}."
        )
        min_len = min(len(expected_result), len(client_result))
    else:
        min_len = expected_len

    for i in range(min_len):
        expected_item = expected_result[i]
        client_item = client_result[i]

        expected_feeling = expected_item.get("feeling")
        expected_ratio = expected_item.get("ratio")

        client_feeling = client_item.get("feeling")
        client_ratio = client_item.get("ratio")

        if "feeling" not in client_item:
            diffs.append(f"Missing 'feeling' key at index {i}: {client_item}")
        if "ratio" not in client_item:
            diffs.append(f"Missing 'ratio' key at index {i}: {client_item}")

        if client_feeling != expected_feeling:
            diffs.append(
                f"Sentiment mismatch at index {i}: Expected '{expected_feeling}', got '{client_feeling}'."
            )
        else:
            if round(expected_ratio, 4) != round(client_ratio, 4):
                diffs.append(
                    f"Ratio mismatch for '{expected_feeling}' at index {i}: expected {expected_ratio}, got {client_ratio}."
                )

    return diffs


def print_diffs(query_number, diffs):
    if len(diffs) != 0:
        print(f"Diffs query {query_number}:")
        for diff in diffs:
            print(f"❌ {diff}")
    else:
        print(f"✅ Results match in query {query_number}")


def compare_results(expected_results, client_results):
    diffs_query_1 = compare_query_1(expected_results["1"], client_results["1"])
    diffs_query_2 = compare_query_2(expected_results["2"], client_results["2"])
    diffs_query_3 = compare_query_3(expected_results["3"], client_results["3"])
    diffs_query_4 = compare_query_4(expected_results["4"], client_results["4"])
    diffs_query_5 = compare_query_5(expected_results["5"], client_results["5"])
    print_diffs(query_number=1, diffs=diffs_query_1)
    print_diffs(query_number=2, diffs=diffs_query_2)
    print_diffs(query_number=3, diffs=diffs_query_3)
    print_diffs(query_number=4, diffs=diffs_query_4)
    print_diffs(query_number=5, diffs=diffs_query_5)

    success: bool = all_results_match(
        [
            diffs_query_1,
            diffs_query_2,
            diffs_query_3,
            diffs_query_4,
            diffs_query_5,
        ]
    )
    return success


def all_results_match(diffs: list[list[str]]):
    """
    Loops through each diff list, and if there is any diff, it returns False.
    """
    for diff_list in diffs:
        if len(diff_list) != 0:
            return False
    return True


def main():
    if len(sys.argv) < 2:
        print("Missing 'expected output' file")
        return
    expected_output_file = sys.argv[1]
    expected_results = json_file_to_dict(expected_output_file)
    config = configparser.ConfigParser()
    config_file = "config.ini"
    config.read(config_file)
    clients = int(config["CLIENTS"]["CLIENTS"])

    any_client_failed = False

    for i in range(clients):
        print(f"\nEsperando finalización del cliente {i}...")
        if i == 0:
            client_container = "client"
        else:
            client_container = f"client_{i}"
        subprocess.run(
            ["docker", "wait", client_container], check=False, capture_output=True
        )
        client_results_file = f"output/results_{i}.json"
        client_results = json_file_to_dict(client_results_file)
        print(f"========= RESULTADOS CLIENT {i} =========")
        success = compare_results(expected_results, client_results)
        print("=======================================\n\n")
        if not success:
            any_client_failed = True

    if any_client_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
