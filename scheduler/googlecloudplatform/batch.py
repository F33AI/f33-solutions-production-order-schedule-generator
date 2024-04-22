import uuid
import random
import string
import logging
from typing import List, Tuple

from google.cloud import batch_v1
from google.cloud.compute_v1 import MachineTypesClient
from google.cloud.batch_v1.types import JobStatus, AllocationPolicy

class BatchClient:

    def __init__(self, project_id: str, region: str = "us-central1",
        service_account: str = None, network_name: str = None,
        sub_network_name: str = None) -> None:

        self.project_id = project_id
        self.region = region
        self.service_account = service_account
        self.parent = f"projects/{project_id}/locations/{region}"
        self.network_name = network_name or "global/networks/default"
        self.sub_network_name = sub_network_name or f"regions/{region}/subnetworks/default"

        credentials = service_account.Credentials.from_service_account_info(service_account) \
            if service_account else None
        self.client = batch_v1.BatchServiceClient(credentials=credentials)

        # Note: That's the only path that would work with Docker containers
        # See: https://www.googlecloudcommunity.com/gc/Infrastructure-Compute-Storage/
        #      Seeing-new-error-mounting-GCS-bucket-on-Google-Cloud-Batch/m-p/491841/highlight/true
        self.mount_path = "/mnt/disks/share"

    def _generate_job_id(self) -> str:
        # Note: The first characters needs to be: [a-z]
        return random.choice(string.ascii_lowercase) + str(uuid.uuid4())[1:]

    def _get_machine_parameters(self, machine_type: str) -> Tuple[int, int]:
        client = MachineTypesClient()
        zone = self.region + "-a"
        data = client.get(machine_type=machine_type, project=self.project_id, zone=zone)
        return data.guest_cpus, data.memory_mb

    def _validate_compute_parameters(self, machine_type: str, vcpu_per_task: int,
                                     memory_per_task: int, num_of_parallel_tasks: int) -> None:

        max_vcpu, max_memory = self._get_machine_parameters(machine_type)
        vcpu_usage = num_of_parallel_tasks * vcpu_per_task
        memory_usage = num_of_parallel_tasks * memory_per_task

        if vcpu_usage > max_vcpu:
            raise RuntimeError(f"The task needs at least {vcpu_usage} cpus. "
                               f"The current machine ({machine_type}) has only {max_vcpu}.")

        if memory_usage > max_memory:
            raise RuntimeError(f"The task needs at least {memory_usage} memory. "
                               f"The current machine ({machine_type}) has only {max_memory}.")

        if vcpu_usage < max_vcpu:
            logging.warning(f"The defined task will utilize only {vcpu_usage} out of"
                            f" {max_vcpu} vCPUs.")

        if memory_usage < max_memory:
            logging.warning(f"The defined task will utilize only {memory_usage} out of"
                            f" {max_memory} MB memory.")

    def _get_runnable_container_definition(self, container_uri: str, args: List[str],
                                           entrypoint: str = None) -> batch_v1.Runnable:

            runnable = batch_v1.Runnable()
            runnable.container = batch_v1.Runnable.Container()
            runnable.container.image_uri = container_uri
            runnable.container.commands = args

            if entrypoint:
                runnable.container.entrypoint = entrypoint

            return runnable

    def _get_gcs_volume_definition(self, bucket_name: str, mount_path: str
                                   ) -> batch_v1.Volume:

        gcs_volume = batch_v1.Volume()
        gcs_bucket = batch_v1.GCS()
        gcs_bucket.remote_path = bucket_name
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = mount_path
        return gcs_volume

    def _get_compute_resources_definition(self, compute_vcpu: int, memory: int
                                          ) -> batch_v1.ComputeResource:

        resources = batch_v1.ComputeResource()
        resources.cpu_milli = 1000 * compute_vcpu
        resources.memory_mib = memory
        return resources

    def run_container(self, container_uri: str, custom_job_name: str = None,
                      container_args: List[str] = [], container_entrypoint: str = None,
                      bucket_path_to_mount: str = None, task_max_retry: int = 2,
                      compute_vcpu_per_task: int = 1, compute_memory_per_task: int = 1024,
                      task_max_duration: str = "3600s", task_num_parallel_executions: int = 1,
                      machine_type: str = "e2-standard-4") -> str:

        # Generate job name if not provided
        job_name = custom_job_name or self._generate_job_id()
        self._validate_compute_parameters(machine_type, compute_vcpu_per_task,
                                          compute_memory_per_task, task_num_parallel_executions)

        logging.debug(f"{job_name} / A new job has been created.")
        logging.debug(f"{job_name} / Machine type: {machine_type} / "
                     f"Time limit: {task_max_duration}")

        # Define resources
        runnable_objects = [
            self._get_runnable_container_definition(container_uri=container_uri, args=container_args,
                                                    entrypoint=container_entrypoint)
        ]
        storage_volumes = [self._get_gcs_volume_definition(bucket_path_to_mount, self.mount_path)] \
            if bucket_path_to_mount else []
        compute = self._get_compute_resources_definition(compute_vcpu_per_task,
                                                         compute_memory_per_task)

        # Define a single task
        task = batch_v1.TaskSpec()
        task.runnables = runnable_objects
        task.volumes = storage_volumes
        task.compute_resource = compute
        task.max_retry_count = task_max_retry
        task.max_run_duration = task_max_duration

        # Define a group of tasks
        # Note: Tasks are grouped inside a job using TaskGroups.
        #       Currently, it's possible to have only one task group.
        group = batch_v1.TaskGroup()
        group.task_count = task_num_parallel_executions
        group.task_spec = task

        # Define how to allocate resources
        # Note: Policies are used to define on what kind of virtual machines the tasks will run on.
        allocation_policy = batch_v1.AllocationPolicy()

        # Define which network the service should use
        network = AllocationPolicy.NetworkPolicy(
            network_interfaces=[
                AllocationPolicy.NetworkInterface(
                    network=self.network_name,
                    subnetwork=self.sub_network_name,
                )
            ]
        )
        allocation_policy.network = network

        # Define instances params
        policy = batch_v1.AllocationPolicy.InstancePolicy()
        policy.machine_type = machine_type
        instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
        instances.policy = policy
        allocation_policy.instances = [instances]

        # Define job
        job = batch_v1.Job()
        job.task_groups = [group]
        job.allocation_policy = allocation_policy
        job.labels = {}

        # Note: We use Cloud Logging as it's an out of the box available option
        job.logs_policy = batch_v1.LogsPolicy()
        job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

        # Create request
        create_request = batch_v1.CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = self.parent

        # Send request
        return (job_name, self.client.create_job(create_request))

    def list_jobs(self):
        return self.client.list_jobs(parent=self.parent)

    def list_tasks(self):
        raise NotImplementedError("This functionality has not been implemented.")

    def delete_job(self, job_name: str):
        # Note: When a job is deleted it disappears from the system
        #       and checking its status would raise an Exception.
        return self.client.delete_job(name=f"{self.parent}/jobs/{job_name}")

    def get_job(self, job_name: str):
        return self.client.get_job(name=f"{self.parent}/jobs/{job_name}")

    def get_task(self, task_name: str):
        raise NotImplementedError("This functionality has not been implemented yet.")

    def get_job_status(self, job_name: str) -> str:
        job = self.get_job(job_name)
        return job.status.state

    def has_finished(self, job_name: str) -> bool:
        return self.get_job_status(job_name) in [
            JobStatus.State.FAILED,
            JobStatus.State.SUCCEEDED,
            JobStatus.State.DELETION_IN_PROGRESS,
        ]
