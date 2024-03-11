# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 SBA Research.


import numpy as np

from FeatureCloud.app.engine.app import AppState, Role, app_state

from config_state import State
from mocks import mock_fhir_fetch
from utils import (
    dump_all_to_csv,
    filter_results,
    group_results,
    sample_data,
    transform_to_fhir_query,
    write_to_csv,
)


INITIAL_STATE = "initial"
FETCH_DATA_STATE = "fetch"
WRITE_STATE = "write"
AGGREGATE_STATE = "aggregate"
GENERATE_TEST_DATA_STATE = "generate_test_data"
TERMINAL_STATE = "terminal"

INPUT_DIR = "/mnt/input"
OUTPUT_DIR = "/mnt/output"
name = "fc-query-executor"


@app_state(INITIAL_STATE, app_name=name)
class TransformState(State):
    def register(self):
        self.register_transition(FETCH_DATA_STATE)

    def run(self):
        self.lazy_init()
        self.read_config()
        self.finalize_config()

        client = self.config["client"]
        fhir_server_list = self.config["fhir_servers"]
        query = self.config["input"]["query"]

        self.log(f"Query is {query}")
        self.log("Transforming to query string.")

        query_uri_string = transform_to_fhir_query(query)
        self.log(f"Transformed query is {query_uri_string}")

        result_file = f"{self.output_dir}/{self.config['results']['file']}"

        # Register variables for next states
        self.store("client", client)
        self.store("fhir_server_list", fhir_server_list)
        self.store("query", query)
        self.store("query_uri_string", query_uri_string)
        self.store("result_file", result_file)
        return FETCH_DATA_STATE


@app_state(FETCH_DATA_STATE)
class FetchState(AppState):
    def register(self):
        self.register_transition(WRITE_STATE)

    def run(self):
        # NOTE: Here, we would need to use the transformed query to make an HTTP call
        # to the FHIR servers. For testing purposes we are mocking the request.
        # We also assume that only Observations are being searched.
        responses = mock_fhir_fetch(
            client=self.load("client"),
            resource_type="Observation",
            servers=self.load("fhir_server_list"),
        )
        # TODO: here the actual query URI string must be passed
        filtered_results = filter_results(self.load("query"), responses)
        # group counts per server
        result_count = group_results(filtered_results)
        for server, count in result_count.items():
            self.log(f"Server {server}: {count} results")

        self.store("filtered_results", filtered_results)
        return WRITE_STATE


@app_state(WRITE_STATE)
class WriteResultsState(AppState):
    def register(self):
        self.register_transition(AGGREGATE_STATE, Role.COORDINATOR)
        self.register_transition(TERMINAL_STATE, Role.PARTICIPANT)

    def run(self):
        filtered_results = self.load("filtered_results")
        result_file = self.load("result_file")

        self.log(f"Writing results to {result_file}")
        if not write_to_csv(filtered_results, result_file):
            raise RuntimeError("Failed to write results.")

        if not self.is_coordinator:
            self.log("Writing local results is done, sending data to coordinator.")
        self.send_data_to_coordinator(data={self.id: filtered_results})

        if self.is_coordinator:
            self.log("Writing local results is done, transitioning to aggregate state.")
            return AGGREGATE_STATE

        return TERMINAL_STATE


@app_state(AGGREGATE_STATE, role=Role.COORDINATOR)
class AgreggateState(AppState):

    def register(self):
        self.register_transition(GENERATE_TEST_DATA_STATE, Role.COORDINATOR)

    def run(self):
        data = self.gather_data()

        # to properly get the data, we have to convert it to a np array
        data_np = [np.array(d) for d in data]

        if not dump_all_to_csv(data=data_np):
            raise RuntimeError("Failed to write aggregated results.")

        self.log("Aggregated results successfully, transitioning to next state.")
        return GENERATE_TEST_DATA_STATE


@app_state(GENERATE_TEST_DATA_STATE, role=Role.COORDINATOR)
class GenerateTestDataState(AppState):

    def register(self):
        self.register_transition(TERMINAL_STATE, Role.COORDINATOR)

    def run(self):
        if not sample_data():
            raise RuntimeError("Failed to generate test data.")

        self.log("Generated test data successfully, transitioning to terminal state.")
        return TERMINAL_STATE
