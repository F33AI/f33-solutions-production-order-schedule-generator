import time
import json
import string
from uuid import uuid4
import pandas as pd
from io import StringIO
from threading import Thread
from typing import Any, Dict, List

from scheduler.names import get_random_name
from scheduler import graphs
from scheduler.googlecloudplatform import \
    BatchClient, CloudStorageClient, DatastoreClient, JobStatus, \
    CloudBuildClient, ArtifactsRegistryClient
from scheduler.datastructures import Experiment, Scenario


SCHEDULER_ZIP_LOCAL = "artifacts/scheduler.zip"
SCHEDULER_ZIP_REMOTE = "f33-solutions/files/scheduler.zip"

class Scheduler:

    def __init__(self, project_id: str, region: str, bucket_name: str,
                 entity_name: str, artifacts_repository_name: str,
                 batch_machine_type: str, service_account: str = None) -> None:

        # Data
        self.project_id = project_id
        self.region = region
        self.bucket_name = bucket_name
        self.artifacts_repository_name = artifacts_repository_name
        self.batch_machine_type = batch_machine_type

        # State
        self._experiments = []

        # Services
        self.batch = BatchClient(project_id, region, service_account)
        self.storage = CloudStorageClient()
        self.datastore = DatastoreClient(entity_name)
        self.artifacts_registry = ArtifactsRegistryClient(project_id, region,
                                                          artifacts_repository_name)
        self.build = CloudBuildClient(project_id)

        # Threads
        self.update_job_status_thread = Thread(
            target=self.update_jobs_state,
            args=[True]
        )
        self.update_job_status_thread.start()


    @property
    def experiments(self) -> List[Any]:
        return self._experiments

    @property
    def container_uri(self) -> str:
        return (f"{self.region}-docker.pkg.dev/{self.project_id}/"
                f"{self.artifacts_repository_name}/scheduler:latest")

    def run(self, job_name: str, cloud_data_dir: str, args: Dict[str, str]):
        num_vcpus, memory_size = self.batch._get_machine_parameters(self.batch_machine_type)
        self.batch.run_container(
            custom_job_name=job_name,
            container_uri=self.container_uri,
            container_args=args,
            bucket_path_to_mount=cloud_data_dir,
            compute_vcpu_per_task=num_vcpus,
            compute_memory_per_task=memory_size,
            machine_type=self.batch_machine_type
        )

    def run_experiment(self, experiment_name: str, jobs: bytes, scenarios: bytes):

        def _get_random_id(): return str(uuid4())[:8]
        def _generate_bucket_path(experiment_name, scenario_name):
            return (
                f"{self.bucket_name}/f33-solution-factory-scheduler/"
                f"experiments/{experiment_name}/{scenario_name}"
            )

        # Iterate over scenarios
        file_like_data = StringIO(str(scenarios, "utf-8"))
        scenarios_df = pd.read_csv(file_like_data)

        experiment = Experiment(experiment_name=experiment_name)

        for _, row in scenarios_df.iterrows():
            scenario_parameters = row.to_dict()
            scenario_name = scenario_parameters["name"]

            # Upload files onto the storage
            storage_path = _generate_bucket_path(experiment_name, scenario_name)
            self.storage.upload_from_bytes(f"{storage_path}/jobs.json", jobs)
            self.storage.upload_content(f"{storage_path}/params.json",
                                        json.dumps(scenario_parameters))

            # Run the solver
            def _remove_nonascii(text: str):
                text = text.lower()
                valid_characters = set(string.ascii_lowercase + string.digits)
                return "".join([letter for letter in text if letter in valid_characters])

            job_name = [experiment_name, scenario_name, _get_random_id()]
            job_name = "-".join([_remove_nonascii(part) for part in job_name])
            args = ["--jobs", "jobs.json", "--parameters", "params.json"]
            self.run(job_name=job_name, cloud_data_dir=storage_path, args=args)

            scenario = Scenario(
                scenario_name=scenario_name,
                batch_job_name=job_name,
                params=scenario_parameters,
                remote_data_path=storage_path
            )
            experiment.scenarios.append(scenario)

        # Add entry to a local tracker
        self._experiments.append(experiment)

    def get_logs_url(self, scenario: Scenario):
        return (
            "https://console.cloud.google.com/batch/"
            f"jobsDetail/regions/{self.region}/"
            f"jobs/{scenario.batch_job_name}/logs?project={self.project_id}"
        )

    def check_if_scheduler_image_exists(self):
        return self.artifacts_registry.check_if_container_exists(
            self.container_uri)

    def build_scheduler_container(self):
        self.storage.upload_file(SCHEDULER_ZIP_LOCAL,
                                 f"{self.bucket_name}/{SCHEDULER_ZIP_REMOTE}",
                                 overwrite=True)
        self.build.build_container_from_archive(self.bucket_name,
                                                SCHEDULER_ZIP_REMOTE,
                                                self.container_uri)

    def get_artifacts_url(self, scenario: Scenario):
        path = scenario.remote_data_path
        return f"https://console.cloud.google.com/storage/browser/{path}"

    def render_gantt_chart(self, scenario: Scenario):
        return graphs.render_gantt_chart(scenario)

    def render_radar_plot(self, experiment: Experiment):
        return graphs.render_radar_plot(experiment)

    def delete_experiment(self, to_delete: Experiment):
        self._experiments.remove(to_delete)

    def get_random_name(self):
        return get_random_name()

    def update_jobs_state(self, loop: bool = False, sleep_in_secs: int = 10):
        while(True):
            for experiment in self._experiments:
                experiment_new_status = []

                for scenario in experiment.scenarios:

                    has_changed_its_status = False

                    # Refresh the status
                    if scenario.status < 4:
                        job_status = self.batch.get_job_status(scenario.batch_job_name)
                        has_changed_its_status = (scenario.status != job_status)
                        scenario.status = job_status

                    # Download metrics if the status has changed to SUCCEEDED
                    if scenario.status == JobStatus.State.SUCCEEDED and has_changed_its_status:
                        metrics_remote_path = scenario.remote_data_path + "/metrics.json"
                        results_remote_path = scenario.remote_data_path + "/results.json"

                        scenario.metrics = self.storage.download_content(
                            metrics_remote_path, json.loads)
                        scenario.results = self.storage.download_content(
                            results_remote_path, json.loads
                        )

                    # Calculate the new experiment status
                    experiment_new_status.append(scenario.status)

                experiment.status = min(experiment_new_status) if len(experiment_new_status) \
                    else experiment.status

            if not loop:
                break

            time.sleep(sleep_in_secs)
