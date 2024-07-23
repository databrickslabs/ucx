"""Drop-in replacement/extension of the WarehousesAPI from the Databricks SDK.

This is currently needed because the SDK does not provide support for the 'disable_uc' property which we need to work with.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from databricks.sdk.service._internal import _repeated_dict, Wait
from databricks.sdk.service.sql import (
    Channel,
    CreateWarehouseRequestWarehouseType,
    CreateWarehouseResponse,
    EditWarehouseRequestWarehouseType,
    EditWarehouseResponse,
    EndpointInfo,
    EndpointTags,
    GetWarehouseResponse,
    ListWarehousesResponse,
    SpotInstancePolicy,
    WarehousesAPI,
)


@dataclass
class EndpointInfoExt(EndpointInfo):
    disable_uc: bool = False

    def as_dict(self) -> dict[str, Any]:
        result = super().as_dict()
        result["disable_uc"] = self.disable_uc
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> EndpointInfoExt:
        self: EndpointInfoExt = super().from_dict(d)  # type: ignore
        self.disable_uc = d.get("disable_uc", False)
        return self


@dataclass
class ListWarehousesResponseExt(ListWarehousesResponse):
    warehouses: list[EndpointInfoExt] | None = None  # type: ignore
    """A list of warehouses and their configurations."""

    def as_dict(self) -> dict:
        """Serializes the ListWarehousesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.warehouses:
            body['warehouses'] = [v.as_dict() for v in self.warehouses]
        return body

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ListWarehousesResponseExt:
        """Deserializes the ListWarehousesResponse from a dictionary."""
        return cls(warehouses=_repeated_dict(d, 'warehouses', EndpointInfoExt))


@dataclass
class GetWarehouseResponseExt(GetWarehouseResponse):
    disable_uc: bool = False

    def as_dict(self) -> dict[str, Any]:
        result = super().as_dict()
        result["disable_uc"] = self.disable_uc
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> GetWarehouseResponseExt:
        self: GetWarehouseResponseExt = super().from_dict(d)  # type: ignore
        self.disable_uc = d.get("disable_uc", False)
        return self


class WarehousesAPIExt(WarehousesAPI):
    """Custom version of the Warehouses API that is aware of the 'disable_uc' property not currently supported by the SDK."""

    def list(self, *, run_as_user_id: int | None = None) -> Iterable[EndpointInfoExt]:  # type: ignore
        query = {}
        if run_as_user_id is not None:
            query['run_as_user_id'] = run_as_user_id
        headers = {
            'Accept': 'application/json',
        }

        json = self._api.do('GET', '/api/2.0/sql/warehouses', query=query, headers=headers)
        parsed = ListWarehousesResponseExt.from_dict(json).warehouses
        return parsed if parsed is not None else []

    def get(self, id: str) -> GetWarehouseResponseExt:  # pylint: disable=redefined-builtin
        headers = {
            'Accept': 'application/json',
        }

        res = self._api.do('GET', f'/api/2.0/sql/warehouses/{id}', headers=headers)
        return GetWarehouseResponseExt.from_dict(res)

    def create(  # type: ignore # pylint: disable=too-complex,too-many-arguments
        self,
        *,
        auto_stop_mins: int | None = None,
        channel: Channel | None = None,
        cluster_size: str | None = None,
        creator_name: str | None = None,
        disable_uc: bool | None = None,
        enable_photon: bool | None = None,
        enable_serverless_compute: bool | None = None,
        instance_profile_arn: str | None = None,
        max_num_clusters: int | None = None,
        min_num_clusters: int | None = None,
        name: str | None = None,
        spot_instance_policy: SpotInstancePolicy | None = None,
        tags: EndpointTags | None = None,
        warehouse_type: CreateWarehouseRequestWarehouseType | None = None,
    ) -> Wait[GetWarehouseResponseExt]:
        body: dict[str, Any] = {}
        if auto_stop_mins is not None:
            body['auto_stop_mins'] = auto_stop_mins
        if channel is not None:
            body['channel'] = channel.as_dict()
        if cluster_size is not None:
            body['cluster_size'] = cluster_size
        if creator_name is not None:
            body['creator_name'] = creator_name
        if disable_uc is not None:
            body['disable_uc'] = disable_uc
        if enable_photon is not None:
            body['enable_photon'] = enable_photon
        if enable_serverless_compute is not None:
            body['enable_serverless_compute'] = enable_serverless_compute
        if instance_profile_arn is not None:
            body['instance_profile_arn'] = instance_profile_arn
        if max_num_clusters is not None:
            body['max_num_clusters'] = max_num_clusters
        if min_num_clusters is not None:
            body['min_num_clusters'] = min_num_clusters
        if name is not None:
            body['name'] = name
        if spot_instance_policy is not None:
            body['spot_instance_policy'] = spot_instance_policy.value
        if tags is not None:
            body['tags'] = tags.as_dict()
        if warehouse_type is not None:
            body['warehouse_type'] = warehouse_type.value
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        op_response = self._api.do('POST', '/api/2.0/sql/warehouses', body=body, headers=headers)
        return Wait(
            self.wait_get_warehouse_running,
            response=CreateWarehouseResponse.from_dict(op_response),
            id=op_response['id'],
        )

    def create_and_wait(  # pylint: disable=too-many-arguments
        self,
        *,
        auto_stop_mins: int | None = None,
        channel: Channel | None = None,
        cluster_size: str | None = None,
        creator_name: str | None = None,
        disable_uc: bool | None = None,
        enable_photon: bool | None = None,
        enable_serverless_compute: bool | None = None,
        instance_profile_arn: str | None = None,
        max_num_clusters: int | None = None,
        min_num_clusters: int | None = None,
        name: str | None = None,
        spot_instance_policy: SpotInstancePolicy | None = None,
        tags: EndpointTags | None = None,
        warehouse_type: CreateWarehouseRequestWarehouseType | None = None,
        timeout=timedelta(minutes=20),
    ) -> GetWarehouseResponseExt:
        return self.create(
            auto_stop_mins=auto_stop_mins,
            channel=channel,
            cluster_size=cluster_size,
            creator_name=creator_name,
            disable_uc=disable_uc,
            enable_photon=enable_photon,
            enable_serverless_compute=enable_serverless_compute,
            instance_profile_arn=instance_profile_arn,
            max_num_clusters=max_num_clusters,
            min_num_clusters=min_num_clusters,
            name=name,
            spot_instance_policy=spot_instance_policy,
            tags=tags,
            warehouse_type=warehouse_type,
        ).result(timeout=timeout)

    def edit(  # type: ignore # pylint: disable=too-complex,too-many-arguments
        self,
        id: str,  # pylint: disable=redefined-builtin
        *,
        auto_stop_mins: int | None = None,
        channel: Channel | None = None,
        cluster_size: str | None = None,
        creator_name: str | None = None,
        disable_uc: bool | None = None,
        enable_photon: bool | None = None,
        enable_serverless_compute: bool | None = None,
        instance_profile_arn: str | None = None,
        max_num_clusters: int | None = None,
        min_num_clusters: int | None = None,
        name: str | None = None,
        spot_instance_policy: SpotInstancePolicy | None = None,
        tags: EndpointTags | None = None,
        warehouse_type: EditWarehouseRequestWarehouseType | None = None,
    ) -> Wait[GetWarehouseResponseExt]:
        body: dict[str, Any] = {}
        if auto_stop_mins is not None:
            body['auto_stop_mins'] = auto_stop_mins
        if channel is not None:
            body['channel'] = channel.as_dict()
        if cluster_size is not None:
            body['cluster_size'] = cluster_size
        if creator_name is not None:
            body['creator_name'] = creator_name
        if disable_uc is not None:
            body['disable_uc'] = disable_uc
        if enable_photon is not None:
            body['enable_photon'] = enable_photon
        if enable_serverless_compute is not None:
            body['enable_serverless_compute'] = enable_serverless_compute
        if instance_profile_arn is not None:
            body['instance_profile_arn'] = instance_profile_arn
        if max_num_clusters is not None:
            body['max_num_clusters'] = max_num_clusters
        if min_num_clusters is not None:
            body['min_num_clusters'] = min_num_clusters
        if name is not None:
            body['name'] = name
        if spot_instance_policy is not None:
            body['spot_instance_policy'] = spot_instance_policy.value
        if tags is not None:
            body['tags'] = tags.as_dict()
        if warehouse_type is not None:
            body['warehouse_type'] = warehouse_type.value
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        op_response = self._api.do('POST', f'/api/2.0/sql/warehouses/{id}/edit', body=body, headers=headers)
        return Wait(self.wait_get_warehouse_running, response=EditWarehouseResponse.from_dict(op_response), id=id)

    def edit_and_wait(  # pylint: disable=too-many-arguments
        self,
        id: str,  # pylint: disable=redefined-builtin
        *,
        auto_stop_mins: int | None = None,
        channel: Channel | None = None,
        cluster_size: str | None = None,
        creator_name: str | None = None,
        disable_uc: bool | None = None,
        enable_photon: bool | None = None,
        enable_serverless_compute: bool | None = None,
        instance_profile_arn: str | None = None,
        max_num_clusters: int | None = None,
        min_num_clusters: int | None = None,
        name: str | None = None,
        spot_instance_policy: SpotInstancePolicy | None = None,
        tags: EndpointTags | None = None,
        warehouse_type: EditWarehouseRequestWarehouseType | None = None,
        timeout=timedelta(minutes=20),
    ) -> GetWarehouseResponseExt:
        return self.edit(
            auto_stop_mins=auto_stop_mins,
            channel=channel,
            cluster_size=cluster_size,
            creator_name=creator_name,
            disable_uc=disable_uc,
            enable_photon=enable_photon,
            enable_serverless_compute=enable_serverless_compute,
            id=id,
            instance_profile_arn=instance_profile_arn,
            max_num_clusters=max_num_clusters,
            min_num_clusters=min_num_clusters,
            name=name,
            spot_instance_policy=spot_instance_policy,
            tags=tags,
            warehouse_type=warehouse_type,
        ).result(timeout=timeout)
