import os
import json
import random
import typer
from solver import solve_flexible_jobshop_problem

def _random():
    return round(random.random(), 2)

def main(jobs: str = None, parameters: str = None):

    DATA_DIR = os.environ.get("DATA_DIR", "")

    # Print all the input parameters
    debug_log = [
        "[ Input parameters: ]",
        f"  * 'jobs' = {jobs}",
        f"  * 'parameters' = {parameters}",
        "[ Env variables: ]",
        f"  * 'DATA_DIR' = {DATA_DIR}"
    ]
    print("\n".join(debug_log))

    # Define paths
    input_jobs_data = os.path.join(DATA_DIR, jobs)
    input_parameters = os.path.join(DATA_DIR, parameters)
    output_results_json = os.path.join(DATA_DIR, "results.json")
    output_metrics_json = os.path.join(DATA_DIR, "metrics.json")


    # Read parameters
    parameters_as_str = open(input_parameters, "r").read()
    parameters = json.loads(parameters_as_str)

    # Read jobs
    input_jobs_data_as_str = open(input_jobs_data, "r").read()
    jobs_data = json.loads(input_jobs_data_as_str)

    # Run the solver
    plotly_data, metrics = solve_flexible_jobshop_problem(
        jobs_data, parameters["objective_function"])

    # Dump the solution
    with open(output_results_json, "w") as output:
        output.write(json.dumps(plotly_data))

    # Dump metrics
    with open(output_metrics_json, "w") as outfile:
        outfile.write(json.dumps(metrics, indent=4))

typer.run(main)