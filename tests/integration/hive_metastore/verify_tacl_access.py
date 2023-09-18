#!/usr/bin/env python
import sys

from databricks.sdk.service.compute import ResultType


def main():
    from databricks.sdk import WorkspaceClient

    table_name = sys.argv[1]

    # labs-aws-simple-spn is a config profile that has SPN
    # with USER access to a workspace and TACL cluster configured
    from databricks.sdk.service.compute import Language

    w = WorkspaceClient(profile="labs-aws-simple-spn")

    ctx = w.command_execution.create(cluster_id=w.config.cluster_id, language=Language.SQL).result()

    res = w.command_execution.execute(
        cluster_id=w.config.cluster_id,
        context_id=ctx.id,
        language=Language.SQL,
        command=f"SELECT * FROM {table_name}",
    ).result()

    if res.results.result_type == ResultType.ERROR:
        raise ValueError(res.results.summary)

    print(res.results.data)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
