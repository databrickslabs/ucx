# Common Challenges and the Solutions
Users might encounter some challenges while installing and executing UCX. Please find the listing of some common challenges and the solutions below.



### Network Connectivity Issues

**From local machine to the Databricks Account and Workspace:** UCX
installation process has to be run from the local laptop using
Databricks CLI and it will deploy the latest version of UCX into the
Databricks workspace. For this reason, the Databricks account and
workspace needs to be accessible from the laptop. Sometimes, the
workspace might have a network isolation, like it can only be reached
from a VPC, or from a specific IP range.

**Solution:** Please check that your laptop has network connectivity to
the Databricks account and workspace. If not, you might need to be
connected to a VPN or configure an HTTP proxy to access your workspace.

**From local machine to GitHub:** UCX needs internet access to connect to GitHub (https://api.github.com
and https://raw.githubusercontent.com) for downloading the tool from the machine running the installation. The
installation will fail if there is no internet connectivity to these URLs.

**Solution:** Ensure that GitHub is reachable from the local machine. If
not, make necessary changes to the network/firewall settings.

**From Databricks workspace to PyPi:** There are some dependent
libraries which need to be installed from
[<u>pypi.org</u>](https://pypi.org/) to run the UCX workflows from the
Databricks workspace. If the workspace doesn’t have network
connectivity, then the job might fail with
NO_MATCHING_DISTRIBUTION_ERROR.

**Solution:** Version 0.24.0 of UCX supports workspace with no internet
access. Please upgrade UCX and rerun the installation. Reply *yes* to
the question "Does the given workspace block Internet access?" asked
during installation. It will then upload all necessary dependencies to
the workspace. Also, please note that UCX uses both UC and non-UC
enabled clusters. If you have different proxy settings for each, then
please update the necessary proxies (eg. with init scripts) for each
cluster type.

**Local machine to Databricks Account and Workspace connection failed due to proxy and self-signed cert:**
When customer uses web proxy and self-singed certification, UCX may not be able to connect to Account and Workspace
with following errors:
```
File "/Users/userabc/.databricks/labs/ucx/state/venv/lib/python3.10/site-packages/urllib3/connectionpool.py", line 466, in _make_request
    self._validate_conn(conn)
  File "/Users/userabc/.databricks/labs/ucx/state/venv/lib/python3.10/site-packages/urllib3/connectionpool.py", line 1095, in _validate_conn
    conn.connect()
  File "/Users/userabc/.databricks/labs/ucx/state/venv/lib/python3.10/site-packages/urllib3/connection.py", line 652, in connect
    sock_and_verified = _ssl_wrap_socket_and_match_hostname(
  File "/Users/userabc/.databricks/labs/ucx/state/venv/lib/python3.10/site-packages/urllib3/connection.py", line 805, in _ssl_wrap_socket_and_match_hostname
    ssl_sock = ssl_wrap_socket(
  File "/Users/userabc/.databricks/labs/ucx/state/venv/lib/python3.10/site-packages/urllib3/util/ssl_.py", line 465, in ssl_wrap_socket
    ssl_sock = _ssl_wrap_socket_impl(sock, context, tls_in_tls, server_hostname)
  File "/Users/userabc/.databricks/labs/ucx/state/venv/lib/python3.10/site-packages/urllib3/util/ssl_.py", line 509, in _ssl_wrap_socket_impl
    return ssl_context.wrap_socket(sock, server_hostname=server_hostname)
  File "/opt/homebrew/Cellar/python@3.10/3.10.14/Frameworks/Python.framework/Versions/3.10/lib/python3.10/ssl.py", line 513, in wrap_socket
    return self.sslsocket_class._create(
  File "/opt/homebrew/Cellar/python@3.10/3.10.14/Frameworks/Python.framework/Versions/3.10/lib/python3.10/ssl.py", line 1104, in _create
    self.do_handshake()
  File "/opt/homebrew/Cellar/python@3.10/3.10.14/Frameworks/Python.framework/Versions/3.10/lib/python3.10/ssl.py", line 1375, in do_handshake
    self._sslobj.do_handshake()
ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate in certificate chain (_ssl.c:1007)
```

**Solution:**  set both `REQUESTS_CA_BUNDLE` and `CURL_CA_BUNDLE`
[to force requests library to set verify=False](https://github.com/psf/requests/blob/8c211a96cdbe9fe320d63d9e1ae15c5c07e179f8/requests/sessions.py#L718)
as well as set `SSL_CERT_DIR` env var pointing to the proxy CA cert for the urllib3 library.



### Insufficient Privileges

**User is not a Databricks workspace administrator:** User running the
installation needs to be a workspace administrator as the CLI will
deploy the UCX tool into the workspace, create jobs, and dashboards.

**Solution:** Identify a workspace admin from your team and ask them to
install UCX with their authentication, or request a workspace
administrator to grant you temporary administrator privileges to run the
installation. More details on the issues that you can run into if
you are not an admin (and some possible solutions) can be found
[here](./troubleshooting.md#resolving-common-errors-on-ucx-install).

**User is not a Cloud IAM Administrator:** Cloud CLI needs to be
installed in the local machine for certain cloud related activities,
like creating an [<u>uber
principal</u>](#create-uber-principal-command).
For this, the user needs Cloud IAM Administrator privileges.

**Solution:** Work with a cloud administrator in your organization to
run the commands that need cloud administrator rights.

Admin privileges required for commands:

| **CLI command**                                                                                  | **Admin privileges** |
|--------------------------------------------------------------------------------------------------|----|
| [<u>install</u>](#install-ucx)                                                                   | Workspace Admin |
| [<u>account install</u>](#advanced-installing-ucx-on-all-workspaces-within-a-databricks-account) | Account Admin |
| [<u>create-account-groups</u>](#create-account-groups-command)                                   | Account Admin |
| [<u>validate-groups-membership</u>](#validate-groups-membership-command)                         | Account Admin |
| [<u>create-uber-principal</u>](#create-uber-principal-command)                                   | Cloud Admin |
| [<u>principal-prefix-access</u>](#principal-prefix-access-command)                               | Cloud Admin |
| [<u>create-missing-principals</u>](#create-missing-principals-command-aws-only)                  | Cloud Admin |
| [<u>delete-missing-principals</u>](#delete-missing-principals-command-aws-only)                  | Cloud Admin |
| [<u>migrate-credentials</u>](#migrate-credentials-command)                                       | Cloud Admin, Account Admin / Metastore Admin / CREATE STORAGE CREDENTIAL privilege |
| [<u>migrate-location</u>](#migrate-locations-command)                                            | Metastore Admin / CREATE EXTERNAL LOCATION privilege |
| [<u>create-catalogs-schemas</u>](#create-catalogs-schemas-command)                               | Metastore Admin / CREATE CATALOG privilege |
| [<u>sync-workspace-info</u>](#sync-workspace-info-command)                                       | Account Admin |
| [<u>manual-workspace-info</u>](#manual-workspace-info-command)                                   | Workspace Admin |



### Version Issues

**Python:** UCX needs Python version 3.10 or later.

**Solution:** Check the current version using `python --version`. If the
version is lower than 3.10, upgrade the local Python version to 3.10 or
higher.

**Databricks CLI:** Databricks CLI v0.213 or higher is needed.

**Solution:** Check the current version with `databricks --version`. For
lower versions of CLI,
[<u>update</u>](https://docs.databricks.com/en/dev-tools/cli/install.html#update)
the Databricks CLI on the local machine.

**UCX:** When you install UCX, you get the latest version. But since UCX
is being actively developed, new versions are released frequently. There
might be issues if you have run the assessment with a much earlier
version, and then trying to run the migration workflows with the latest
UCX version.

**Solution:** Upgrade UCX, and rerun the assessment job before running
the migration workflows. For some reason, if you want to install a
specific version of UCX, you can do it using the command
`databricks labs install ucx@\<version\>`, for example,
`databricks labs install ucx@v0.21.0`.



### Authentication Issues

**Workspace Level:** If you are facing authentication issues while
setting up Databricks CLI, please refer to the
[Cryptic errors on authentication](./troubleshooting.md#cryptic-errors-on-authentication)
section to resolve the common errors related to authentication,
profiles, and tokens.

**Account Level:** Not only workspace, but account level authentication
is also needed for installing UCX. If you do not have an account
configured in .databrickscfg, you will get an error message
".databrickscfg does not contain account profiles; please create one
first".

**Solution:** To authenticate with a Databricks account, consider using
one of the following authentication types: [<u>OAuth machine-to-machine
(M2M)
authentication</u>](https://docs.databricks.com/en/dev-tools/cli/authentication.html#m2m-auth),
[<u>OAuth user-to-machine (U2M)
authentication</u>](https://docs.databricks.com/en/dev-tools/cli/authentication.html#u2m-auth),
[<u>Basic authentication
(legacy)</u>](https://docs.databricks.com/en/dev-tools/cli/authentication.html#basic-auth).



### Multiple Profiles in Databricks CLI

**Workspace Level:** More than one workspace profile can be configured
in the .databrickscfg file. For example, you can have profiles set for
Dev and Prod workspaces. You want to install UCX only for the Prod
workspace.

**Solution:** The Databricks CLI provides an option to select the
[<u>profile</u>](https://docs.databricks.com/en/dev-tools/cli/profiles.html)
using `--profile \<profile_name\>` or `-p \<profile_name\>`. You can
test that the correct workspace is getting selected by running any
Databricks CLI command. For example, you can run `databricks clusters
list -p prod` and check that the Prod clusters are being returned. Once
the profile is verified, you can run UCX install for that specific
profile: `databricks labs install ucx -p prod`.

**Account Level:** Multiple account level profiles are set in the
.databrickscfg file.

**Solution:** The installation command `databricks labs install ucx`
will provide an option to select one account profile.



### Workspace has an external Hive Metastore (HMS)

**External HMS connectivity from UCX clusters:** If the workspace has an
external HMS, the clusters running the UCX jobs need to have specific
configurations to connect to the external HMS. Otherwise, UCX assessment
will not be able to assess the tables on HMS.

**Solution:** Use a cluster policy before installation to set the
required Spark config for connecting to the external HMS, or manually
edit the cluster post-installation to have the correct configurations.
Detailed steps can be found
[here](./external_hms_glue.md).

**External HMS connectivity from UCX SQL warehouse:** UCX requires a SQL
warehouse to create tables, run queries, create and refresh dashboards.
If you already have a Pro or Serverless warehouse connected to the
external HMS, you can select the same warehouse for UCX. You will also
be given an option (during installation) to create a new warehouse for
UCX. If you have never used a warehouse before, the new warehouse
created might not have proper configuration set to connect to the
external HMS.

**Solution:** Set Spark configuration for connecting to external HMS in
the Admin Settings of SQL warehouse. This will only be needed if the
admin settings do not have the configurations already set. For example,
add *spark.hadoop.javax.jdo.option.ConnectionURL \<connectionstring\>*
under Data Access Configuration of SQL Warehouse Admin Settings.



### Verify the Installation

Once the UCX command `databricks labs install ucx` has completed
successfully, the installation can be verified with the following steps:

1.  Go to the Databricks Catalog Explorer and check if a new schema for
    ucx is available in Hive Metastore with all empty tables.

2.  Check that the UCX jobs are visible under Workflows.

3.  Run the assessment. This will start the UCX clusters, crawl through
    the workspace, and display results in the UCX dashboards. In case of
    external HMS, verify from the results that the assessment has
    analyzed the external HMS tables.


