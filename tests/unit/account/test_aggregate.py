import logging
from databricks.labs.ucx.account.aggregate import AccountAggregate
from databricks.labs.ucx.account.workspaces import AccountWorkspaces


def test_basic_readiness_report(acc_client, caplog):
    account_ws = AccountWorkspaces(acc_client)
    account_aggregate_obj = AccountAggregate(account_ws)

    with caplog.at_level(logging.INFO):
        account_aggregate_obj.readiness_report()

    assert 'UC compatibility' in caplog.text
