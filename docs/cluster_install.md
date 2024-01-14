
# Setup UCX from Databricks web terminal

- Start an *Unassigned* interactive cluster
- Create a new notebook to attach to the cluster
- Open a web terminal, expand to full page for better readability

Install / setup needed UCX dependencies

```shell
apt install python3.10-venv
unset DATABRICKS_RUNTIME_VERSION
```

Download and install databricks cli

```shell
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Set PATH variable to point to new install

```shell
export PATH=/usr/local/bin:$PATH
```

Verify databricks cli

```shell
which databricks
databricks --version
```

Configure databricks cli to authenticate to correct workspace, add host name and PAT token
```shell
databricks configure
```

Add account level config
```shell
vi ~/.databrickcfg
```
and based on your cloud provider, add
```ini
[AZUCX]
host       = https://accounts.azuredatabricks.net
account_id = beefdead-beef-dead-9999-beefdeadbeef
```
or
```ini
[AWSUCX]
host = https://accounts.cloud.databricks.com
account_id = beefdead-beef-dead-beef-beefdeadbeef
```

Verify configuration and databricks labs
```shell
databricks auth env
databricks clusters list
databricks labs list
```

Install UCX, do not select options to open config or README
```shell
databricks labs install ucx
```
