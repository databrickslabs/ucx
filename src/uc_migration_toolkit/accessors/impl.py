from collections.abc import Callable, Iterator

from databricks.sdk.service.compute import (
    ClusterDetails,
    InstancePoolAndStats,
    InstanceProfile,
    Policy,
)
from databricks.sdk.service.jobs import BaseJob
from databricks.sdk.service.ml import Experiment
from databricks.sdk.service.pipelines import PipelineStateInfo
from databricks.sdk.service.settings import TokenInfo
from databricks.sdk.service.sql import Alert, Dashboard, EndpointInfo, Query
from databricks.sdk.service.workspace import ObjectInfo, RepoInfo

from uc_migration_toolkit.accessors.generic.accessor import (
    DatabricksObject,
    GenericAccessor,
)


class ClusterAccessor(GenericAccessor[ClusterDetails]):
    @property
    def object_type(self):
        return "clusters"

    @property
    def listing_function(self):
        return self.ws_client.clusters.list

    @property
    def id_attribute(self) -> str:
        return ClusterDetails.cluster_id


class JobAccessor(GenericAccessor[BaseJob]):
    def object_type(self):
        return "jobs"

    @property
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        return self.ws_client.jobs.list

    @property
    def id_attribute(self) -> str:
        return BaseJob.job_id


class PipelineAccessor(GenericAccessor[PipelineStateInfo]):
    @property
    def object_type(self):
        return "pipelines"

    @property
    def listing_function(self):
        return self.ws_client.pipelines.list_pipelines

    @property
    def id_attribute(self) -> str:
        return PipelineStateInfo.pipeline_id


class ClusterPoliciesAccessor(GenericAccessor[Policy]):
    @property
    def object_type(self):
        return "policies"

    @property
    def listing_function(self):
        return self.ws_client.cluster_policies.list

    @property
    def id_attribute(self) -> str:
        return Policy.policy_id


class ExperimentAccessor(GenericAccessor[Experiment]):
    def object_type(self):
        return "experiments"

    @property
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        return self.ws_client.experiments.list_experiments

    @property
    def id_attribute(self) -> str:
        return Experiment.experiment_id


class InstancePoolAccessor(GenericAccessor[InstancePoolAndStats]):
    def object_type(self):
        return "pools"

    @property
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        return self.ws_client.instance_pools.list

    @property
    def id_attribute(self) -> str:
        return InstancePoolAndStats.instance_pool_id


class SQLWarehousesAccessor(GenericAccessor[EndpointInfo]):
    @property
    def object_type(self):
        return "warehouses"

    @property
    def listing_function(self):
        return self.ws_client.warehouses.list

    @property
    def id_attribute(self) -> str:
        return EndpointInfo.id


class DashboardAccessor(GenericAccessor[Dashboard]):
    @property
    def object_type(self):
        return "dashboards"

    @property
    def listing_function(self):
        return self.ws_client.dashboards.list

    @property
    def id_attribute(self) -> str:
        return Dashboard.id


class QueryAccessor(GenericAccessor[Query]):
    @property
    def object_type(self):
        return "queries"

    @property
    def listing_function(self):
        return self.ws_client.queries.list

    @property
    def id_attribute(self) -> str:
        return Query.id


class AlertsAccessor(GenericAccessor[Alert]):
    @property
    def object_type(self):
        return "alerts"

    @property
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        return self.ws_client.alerts.list

    @property
    def id_attribute(self) -> str:
        return Alert.id


class TokenAccessor(GenericAccessor[TokenInfo]):
    @property
    def object_type(self):
        return "tokens"

    @property
    def listing_function(self):
        return self.ws_client.tokens.list

    @property
    def id_attribute(self) -> str:
        return TokenInfo.token_id


class InstanceProfileAccessor(GenericAccessor[InstanceProfile]):
    @property
    def object_type(self):
        return "instance_profiles"

    @property
    def listing_function(self):
        return self.ws_client.instance_profiles.list

    @property
    def id_attribute(self) -> str:
        return InstanceProfile.instance_profile_arn


class WorkspaceAccessor(GenericAccessor[ObjectInfo]):
    @property
    def id_attribute(self) -> str:
        return ObjectInfo.object_id

    @property
    def object_type(self):
        return "notebooks"

    @property
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        return lambda: self.ws_client.workspace.list("/", recursive=True)


class ReposAccessor(GenericAccessor[RepoInfo]):
    @property
    def id_attribute(self) -> str:
        return RepoInfo.id

    @property
    def object_type(self):
        return "repos"

    @property
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        return lambda: self.ws_client.repos.list
