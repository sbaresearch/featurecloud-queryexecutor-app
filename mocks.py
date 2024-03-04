# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 SBA Research.


import json
import logging
import os


def mock_fhir_fetch(client, resource_type="Observation", servers=[]) -> dict:
    """
    Mock responses for provided server list.

    Parameters:
        client (str): Name representing where client-specific data is stored.
        resource_type (str): FHIR resource type to search for.
        servers (list): List of FHIR servers this client can connect to.

    Returns:
        response (dict): JSON response for each server.
    """
    if not servers:
        logging.warn("No servers were configured for this client.")
        return

    DATA_DIR = os.path.join("/", "app", "data", client, "mock_fhir_data")
    response = {}

    for server in servers:
        server_path = os.path.join(DATA_DIR, server)

        response.update({server: []})

        for filename in os.listdir(server_path):

            if filename.endswith(".json"):
                file_path = os.path.join(server_path, filename)

                with open(file_path, "r") as file:
                    data = json.load(file)

                    for entry in data["entry"]:
                        if entry["resource"]["resourceType"] == resource_type:
                            response[server].append(entry)

    return response
