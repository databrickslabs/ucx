---
linkTitle: "Documentation"
title: Introduction
---

👋 Hello! Welcome to the UCX documentation!

## What is UCX?

UCX is a companion toolkit to assist users with upgrading to Unity Catalog (UC). It is provided in a form of CLI tool that can be used to automate the process of upgrading to UC.


Usual steps to upgrade to UC include infra-level configuration, which is followed by tables and code migration.

Infra-level configuration includes:
- Setting up a UC metastore
- Assess the current state of your workspaces by running UCX assessment
- Attach workspaces to UC metastore
- Migrate workspace-level groups to account-level groups

Tables and code migration includes:
- Setting up cloud-level infra for UC (storage credentials, external locations, volumes)
- Migrate tables to UC catalogs
- Adjust code to be compatible with UC-enabled compute
- Switch clusters to UC-enabled compute
- Change downstream consumers (e.g. BI tools) to use UC-enabled compute and UC catalogs


## Questions or Feedback?

{{< callout emoji="💻" type="info" >}}
  UCX is still in active development.\
  Have a question or feedback? Feel free to [open an issue](https://github.com/databrickslabs/ucx/issues)
{{< /callout >}}

## Next

Dive right into the following section to get started:

{{< cards >}}
  {{< card link="installation" title="Installation" icon="document-text" subtitle="Learn how to install UCX" >}}
  {{< card link="tutorial" title="Tutorial" icon="document-text" subtitle="Learn how to use UCX" >}}
  {{< card link="reference" title="Reference" icon="document-text" subtitle="Explore the UCX CLI reference" >}}
{{< /cards >}}