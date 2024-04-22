import re
from typing import List, Tuple
from google.cloud import artifactregistry_v1
from google.api_core.exceptions import NotFound

class ArtifactsRegistryClient():
    def __init__(self, project_id: str, region: str,
                 repository_name: str) -> None:
        self.project_id = project_id
        self.region = region
        self.repository_name = repository_name
        self.client = artifactregistry_v1.ArtifactRegistryClient()


    def split_docker_uri(self, uri: str) -> Tuple[List[str], str, str]:
        """ Splits Docker URI into: path, name and tag """
        return re.split("/|:", uri)

    def check_if_container_exists(self, container_uri: str,
                                  tag: str = "latest"):

        *_, container_name, _ = self.split_docker_uri(container_uri)
        uri = (f"projects/{self.project_id}/locations/{self.region}/"
               f"repositories/{self.repository_name}/packages/"
               f"{container_name}/tags/{tag}")
        try:
            _ = self.client.get_tag(dict(name=uri))
            return True
        except NotFound:
            return False
