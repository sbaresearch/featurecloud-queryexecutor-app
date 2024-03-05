# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 SBA Research.


import csv

from numpy import ndenumerate
from pandas import read_csv


def transform_to_fhir_query(data):
    fhir_query_parts = []
    last_logical_operator = None

    for key, value in data.items():
        # Remove trailing hyphens (these are denoting the index number)
        attribute_name = "-".join(key.rsplit("-", 1)[:-1])

        if "logical_operator" in value:
            last_logical_operator = value["logical_operator"]

        if last_logical_operator and len(fhir_query_parts) > 0:
            fhir_query_parts.append(
                f'&{attribute_name}{value["operator"]}{value["value"]}'
            )
        else:
            fhir_query_parts.append(
                f'{attribute_name}{value["operator"]}{value["value"]}'
            )

    fhir_query_string = "".join(fhir_query_parts)
    # Replace "and" with "&" in the query string
    fhir_query_string = fhir_query_string.replace("and", "&")
    return "q=" + fhir_query_string


def check_numeric_value(value, operator, numeric_value):
    """
    Check if the numeric value matches the specified criteria.

    Parameters:
        value (float): Numeric value to check.
        operator (str): Operator for comparison ("<", ">", "=", "<=", ">=").
        numeric_value (float): Value to compare against.

    Returns:
        bool: True if the value matches the criteria, False otherwise.
    """
    try:
        value = float(value)
        numeric_value = float(numeric_value)

        if operator == "<":
            return value < numeric_value
        elif operator == ">":
            return value > numeric_value
        elif operator == "=":
            return value == numeric_value
        elif operator == "<=":
            return value <= numeric_value
        elif operator == ">=":
            return value >= numeric_value
        else:
            return False
    except ValueError:
        return False


def filter_results(payload: dict, responses: dict) -> dict:
    """
    Filter JSON response based on provided criteria for each server.

    Parameters:
        json_response (dict): The JSON response containing data from multiple servers.
        payload (dict): Dictionary containing filter criteria.

    Returns:
        dict: Filtered JSON response for each server.
    """
    filtered_dict = {}

    for server, json_list in responses.items():
        filtered_list = []

        for key, conditions in payload.items():
            # Remove trailing hyphens (these are denoting the index number)
            key_prefix = "-".join(key.rsplit("-", 1)[:-1])
            operator = conditions["operator"]
            value = conditions["value"]
            logical_operator = conditions.get("logical_operator")

            for item in json_list:
                resource = item.get("resource", {})
                code = resource.get("code", {}).get("coding", [{}])[0].get("code", "")
                display = (
                    resource.get("code", {}).get("coding", [{}])[0].get("display", "")
                )
                numeric_value = resource.get("valueQuantity", {}).get("value", "")
                unit = resource.get("valueQuantity", {}).get("unit", "")
                issued = resource.get("issued")

                # Check if the code matches the key
                if code == key_prefix:
                    if check_numeric_value(numeric_value, operator, float(value)):
                        filtered_item = {
                            "code": code,
                            "display": display,
                            "numeric_value": numeric_value,
                            "unit": unit,
                            "issued": issued,
                        }
                        filtered_list.append(filtered_item)

        filtered_dict[server] = filtered_list

    return filtered_dict


def group_results_per_server(results: dict) -> dict:
    """
    Group the results per server and count the number of results for each server.

    Parameters:
        results (dict): A dictionary where keys are server names and values are lists of results.

    Returns:
        dict: A dictionary where keys are server names and values are the number of results for each server.
    """
    return {server: len(result_list) for server, result_list in results.items()}


def write_to_csv(filtered_dict, csv_file_path):
    """
    Write filtered results from a dictionary to a CSV file.

    Parameters:
        filtered_dict (dict): Dictionary containing filtered results for each server.
        csv_file_path (str): The file path where the CSV will be saved.

    Returns:
        bool: True if writing to CSV is successful, False otherwise.
    """

    try:
        headers = list(filtered_dict.values())[0][0].keys()

        with open(csv_file_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            # Write each server's filtered results to the CSV file
            for server_name, server_results in filtered_dict.items():
                for result in server_results:
                    writer.writerow(result)
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        return False

    return True


def dump_all_to_csv(
    data: list, csv_file_path: str = "/mnt/output/aggregated_results.csv"
) -> bool:
    """
    Dump data taken from all clients from a list of numpy arrays to a CSV file.
    To be called only from a coordinator state.

    The data is expected to look like this:
    [
        array(
            {
                '<fc_client_id>': {
                    '<server_name>': [
                        {
                            '<attribute>': '<value>',
                        }
                    ]
                }
            }
        ),
        dtype=object,
    ]

    Parameters:
        data (list): List of numpy arrays containing data.
        csv_file_path (str): The file path where the CSV will be saved.

    Returns:
        bool: True if writing to CSV is successful, False otherwise.
    """
    try:
        with open(csv_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            headers = list(list(data[0].flat[0].values())[0].values())[0][0].keys()
            writer.writerow(headers)

            # Iterate over each numpy array
            for array in data:
                # Iterate over each client's servers' responses
                for _, client in ndenumerate(array):
                    # Iterate over each client's servers' responses
                    for client_data in client.values():
                        # Iterate over each server's data for the client
                        for server_data in client_data.values():
                            for entry in server_data:
                                writer.writerow(entry.values())
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        return False

    return True


def sample_data(
    input_file="/mnt/output/aggregated_results.csv",
    output_file="/mnt/output/test_data.csv",
    **kwargs,
):
    """
    Randomly samples a portion of the input training data to create a test dataset.

    Parameters:
        input_file (str): Path to the input CSV file containing the training data. Default is "aggregated_results.csv".
        output_file (str): Path to save the sampled test data as a CSV file. Default is "test_data.csv".
        **kwargs: Additional keyword arguments to be passed to the pandas DataFrame `sample` method.
            - frac (float): Fraction of the training data to sample. Default is 0.2 (20%).
            - random_state (int): Seed for random number generation to ensure reproducibility. Default is 42.
            - Additional arguments accepted by pandas DataFrame `sample` method.

    Returns:
        bool: True if the test data was successfully sampled and saved, False otherwise.

    Example:
        # Sample 30% of the training data and save as test_data.csv
        sample_data(input_file="train_data.csv", output_file="test_data.csv", frac=0.3, random_state=123)
    """
    try:
        train_data = read_csv(input_file)

        frac = kwargs.get("frac") or 0.2
        random_state = kwargs.get("random state") or 42

        # sample a portion of the training data for the test data
        test_data = train_data.sample(frac=frac, random_state=random_state, **kwargs)
        test_data.to_csv(output_file, index=False)
    except Exception as e:
        print(f"Error occurred while sampling and saving test data: {e}")
        return False

    return True
