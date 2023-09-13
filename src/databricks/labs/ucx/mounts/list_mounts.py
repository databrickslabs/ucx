from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import AwsAttributes, Language, AwsAvailability
import logging
import time
from importlib import resources
from pathlib import Path
import json
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MountResult:
    mount_source: str
    mount_path: str
    aws_instance_profile_arn: str
    aws_iam_role_arn: str
    aws_is_meta_instance_profile: str

class MountLister():
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def execute(self):
        instances = self.list_instance_profiles()
        self.list_mounts(instances)

    def list_instance_profiles(self):
        return self._ws.instance_profiles.list()

    def list_mounts(self, instances):
        code = self.read_code()

        data = []
        #TODO: Make it executable in // as sequential execution is terrible
        for instance_profile in instances:
            try:
                cluster = self.create_cluster(instance_profile)
                results = self.execute_code(cluster.cluster_id, instance_profile, code)
                data.append(results)
            except Exception as e:
                logger.info(f"An error occured: {e}")
        return data

    @staticmethod
    def read_code():
        p = Path(__file__).with_name('execute_directory_list.scala')
        code = p.open('r').read()
        return code

    def create_cluster(self, instance_profile):
        cluster_name = f'ucx-{instance_profile.instance_profile_arn.split("/")[1]}'
        aws_attribute = AwsAttributes(
            first_on_demand=1,
            availability=AwsAvailability.SPOT_WITH_FALLBACK,
            instance_profile_arn=instance_profile.instance_profile_arn,
            zone_id="auto",
            ebs_volume_count=0,
            spot_bid_price_percent=100
        )

        logger.info(
            f"Creating cluster {cluster_name} with AWS Instance profile {instance_profile.instance_profile_arn}")

        return self._ws.clusters.create(cluster_name=cluster_name,
                                               aws_attributes=aws_attribute,
                                               node_type_id="i3.xlarge",
                                               spark_version=self._ws.clusters.select_spark_version(latest=True),
                                               autotermination_minutes=10,
                                               num_workers=1).result()

    def execute_code(self, cluster_id, instance, code):
        context = self._ws.command_execution.create(cluster_id=cluster_id,language=Language.SCALA).result()

        logger.info(f"Executing listing code on cluster {cluster_id}")
        #Executing code in scala as it result in a much faster execution than the one in Python.
        #Implementation is took directly from our Terraform provider
        #https://github.com/databricks/terraform-provider-databricks/blob/master/exporter/util.go#L351-L459
        text_results = self._ws.command_execution.execute(cluster_id=cluster_id,
                                                              context_id=context.id,
                                                              language=Language.SCALA,
                                                              command=code).result().results.data.split("\n")[0]
        results_dict = json.loads(text_results)

        results = []
        for mount_source, mount_path in results_dict.items():
            results.append(
                MountResult(
                    mount_source,
                    mount_path,
                    str(instance.instance_profile_arn or ''),
                    str(instance.iam_role_arn or ''),
                    str(instance.is_meta_instance_profile or '')
                )
            )

        return results
