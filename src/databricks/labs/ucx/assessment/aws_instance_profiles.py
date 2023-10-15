from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

@dataclass
class AWSInstanceProfileInfo:
    # The AWS ARN of the instance profile to register with Databricks.
    # This field is required.
    instance_profile_arn: str
    # The AWS IAM role ARN of the role associated with the instance profile.
    # This field is required if your role name and
    # instance profile name do not match and you want to use
    # the instance profile with Databricks SQL Serverless.
    iam_role_arn: str = None
    # Boolean flag indicating whether the instance profile should only be
    # used in credential passthrough scenarios.
    # If true, it means the instance profile contains an meta IAM role
    # which could assume a wide range of roles. Therefore it should
    # always be used with authorization. This field is optional,
    # the default value is false.
    is_meta_instance_profile: bool = False



class AWSInstanceProfileCrawler(CrawlerBase):
    """Crawl AWS workspace for Instance profiles"""

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "aws_instance_profiles", AWSInstanceProfileInfo)
        self._ws = ws

    def _crawl(self) -> list[AWSInstanceProfileInfo]:
        instance_profiles = self._ws.instance_profiles.list()

        for ips in instance_profiles:
            yield AWSInstanceProfileInfo(
                iam_role_arn=ips.iam_role_arn,
                instance_profile_arn=ips.instance_profile_arn,
                is_meta_instance_profile=ips.is_meta_instance_profile,
            )

    def snapshot(self) -> list[AWSInstanceProfileInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[AWSInstanceProfileInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AWSInstanceProfileInfo(*row)
