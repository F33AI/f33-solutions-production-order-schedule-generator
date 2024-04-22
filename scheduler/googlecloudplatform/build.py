from google.cloud.devtools import cloudbuild_v1


class CloudBuildClient():
    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = cloudbuild_v1.CloudBuildClient()

    def build_container_from_archive(self, bucket_name: str, relative_path: str,
                                     container_uri: str):
        source = {
            "storage_source": {
                "bucket": bucket_name,
                "object_": relative_path
            }
        }

        steps = [
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": ["build", "-t", container_uri, "."]
            },
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": ["push", container_uri]
            }
        ]

        build = cloudbuild_v1.Build(source=source, steps=steps)
        job = self.client.create_build(project_id=self.project_id, build=build)
        return job.result()
