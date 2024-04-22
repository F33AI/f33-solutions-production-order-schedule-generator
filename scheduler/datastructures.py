from dataclasses import dataclass, field
from typing import List
from scheduler.googlecloudplatform.batch import JobStatus

@dataclass
class Scenario:
    scenario_name: str
    batch_job_name: str
    params: dict
    remote_data_path: str
    metrics: dict = field(default_factory=dict)
    results: list = field(default_factory=list)
    status: JobStatus.State = JobStatus.State.QUEUED

@dataclass
class Experiment:
    experiment_name: str
    scenarios: List[Scenario] = field(default_factory=list)
    status: JobStatus.State = JobStatus.State.QUEUED
