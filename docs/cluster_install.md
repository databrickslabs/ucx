
# Setup UCX from Databricks web terminal

- Start an *Unassigned* interactive cluster
- Create a new notebook to attch to the cluster
- Open a web terminal, expand to full page for better readability

- Install / setup needed UCX dependencies
`apt install python3.10-venv`
`unset DATABRICKS_RUNTIME_VERSION`

- Download and install databricks cli
`curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`

- Set PATH variable to point to new install
`export PATH=/usr/local/bin:$PATH`

- Verify databricks cli
`which databricks`
`databricks --version`

- Configure databricks cli to authenticate to correct workspace, add host name and PAT token
`databricks configure`

- add account level config
`vi ~/.databrickcfg`
and depending on your cloud, add
```
[AZUCX]
host       = https://accounts.azuredatabricks.net
account_id = beefdead-beef-dead-9999-beefdeadbeef
```
or
```
[AWSUCX]
host = https://accounts.cloud.databricks.com
account_id = beefdead-beef-dead-beef-beefdeadbeef
```

- Verify configuration and databricks labs
`databricks auth env`
`databricks clusters list`
`databricks labs list`

- Install UCX, do not select options to open config or README
`databricks labs install ucx`
