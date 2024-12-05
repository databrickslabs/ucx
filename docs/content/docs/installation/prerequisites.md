---
linkTitle: Prerequisites
title: Prerequisites
weight: 1
---

{{< callout type="info" >}}
To install UCX, user must have Databricks Workspace Administrator privileges.
{{< /callout >}}

{{< callout type="warning" >}}
Running UCX as a Service Principal is not supported.
{{< /callout >}}

### Local machine
- Databricks CLI v0.213 or later. See [instructions](#authenticate-databricks-cli).
- Python 3.10 or later. See [Windows](https://www.python.org/downloads/windows/) instructions.


### Network access from local machine
- Network access to your Databricks Workspace used for the [installation process](#install-ucx).
- Network access to the Internet for [pypi.org](https://pypi.org) and [github.com](https://github.com) from machine running the installation.

### Account-level settings
- Account level Identity Setup. See instructions for [AWS](https://docs.databricks.com/en/administration-guide/users-groups/best-practices.html), [Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/users-groups/best-practices), and [GCP](https://docs.gcp.databricks.com/administration-guide/users-groups/best-practices.html).
- Unity Catalog Metastore Created (per region). See instructions for [AWS](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html), [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-metastore), and [GCP](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html).


### Workspace-level settings
- Workspace should have Premium or Enterprise tier.
- A PRO or Serverless SQL Warehouse to render the [report](../reference/assessment.md) for the [assessment workflow](#assessment-workflow).

{{< callout type="info" >}}
If your Databricks Workspace relies on an external Hive Metastore (such as AWS Glue), make sure to read [this guide](../reference/external_hms_glue.md)
{{< /callout >}}
