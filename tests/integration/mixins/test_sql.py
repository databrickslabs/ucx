from __future__ import annotations

import datetime as dt

import pytest

from databricks.sdk import WorkspaceClient
from databricks.labs.ucx.mixins.sql import WarehousesAPIExt


@pytest.fixture
def warehouses(ws: WorkspaceClient) -> WarehousesAPIExt:
    return WarehousesAPIExt(ws.api_client)


@pytest.mark.parametrize("uc_enabled", (True, False))
def test_sql_warehouse_creation_uc(warehouses: WarehousesAPIExt, uc_enabled: bool, make_warehouse) -> None:
    """Test that we can create a SQL Warehouse with and without UC enabled."""
    disable_uc = not uc_enabled

    # Create the SQL warehouse and verify attribute.
    created = make_warehouse(disable_uc=disable_uc, enable_serverless_compute=True).result(
        timeout=dt.timedelta(minutes=1)
    )
    assert created.disable_uc == disable_uc

    # Verify consistency, that we can independently read the attribute.
    warehouse = warehouses.get(created.id)
    assert warehouse.disable_uc == disable_uc


def test_sql_warehouse_upgrade(warehouses: WarehousesAPIExt, make_warehouse) -> None:
    """Test that we can upgrade a SQL Warehouse to have UC enabled."""

    # Start off with UC disabled.
    created = make_warehouse(disable_uc=True, enable_serverless_compute=True).result(timeout=dt.timedelta(minutes=1))
    assert created.disable_uc

    # Perform the upgrade.
    upgraded = warehouses.edit_and_wait(id=created.id, disable_uc=False)
    assert not upgraded.disable_uc

    # Verify consistency, that we can read our own update.
    assert not warehouses.get(id=created.id).disable_uc


def test_sql_warehouse_list(warehouses, make_warehouse) -> None:
    """Verify that when listing warehouses we can check whether UC is enabled or not."""

    # Create two SQL clusters to find in the listing.
    pending_warehouse_uc_enabled = make_warehouse(disable_uc=False, enable_serverless_compute=True)
    pending_warehouse_uc_disabled = make_warehouse(disable_uc=True, enable_serverless_compute=True)
    warehouse_uc_enabled = pending_warehouse_uc_enabled.result(timeout=dt.timedelta(minutes=1))
    warehouse_uc_disabled = pending_warehouse_uc_disabled.result(timeout=dt.timedelta(minutes=1))

    # List all the clusters.
    all_warehouses = warehouses.list()

    # Check ours are in there and that the UC state is correct.
    found_warehouse_uc_enabled = next(w for w in all_warehouses if w.id == warehouse_uc_enabled.id)
    assert not found_warehouse_uc_enabled.disable_uc
    found_warehouse_uc_disabled = next(w for w in all_warehouses if w.id == warehouse_uc_disabled.id)
    assert found_warehouse_uc_disabled.disable_uc
