# Contributing

The following principles hold:

- this project has to use Databricks SDK for Python.
- anything that doesn't fit into WorkspaceClient has to get through a "mixin" process to get to the SDK itself eventually.
- you can see the example for mixins based on the `StatementExecutionExt`.

Code organization:

- components that require API interaction must be split from the components doing business logic on a class level.
- the amount of logic in the API-calling components should be kept to a minimum.
- prefer injecting business logic components into API-calling components.
- all business logic has to be covered with unit tests. It should be easier without any API calls to mock.

Integration tests:

- all new code has to be covered by integration tests.
- integration tests should use predefined test fixtures provided in the environment variables.
- tests that require their own unique fixture setup must limit the wall clock time of fixture initialization to under one second.
- each integration test must be debuggable in IntelliJ IDEA (Community Edition) with the Python plugin (community edition).

IDE setup:

- The only supported IDE for developing this project is based on IntelliJ. This means that both PyCharm (commercial) and IntelliJ IDEA (Community Edition) with Python plugin (community edition) are supported.
- VSCode is not currently supported, as debugging a single integration test from it is impossible. This may change in the future.

## Development

This section describes setup and development process for the project.

### Local setup

- Install [hatch](https://github.com/pypa/hatch):

```shell
pip install hatch
```

- Create environment:

```shell
hatch env create
```

- Install dev dependencies:

```shell
hatch run pip install -e '.[test,dbconnect]'
```

- Pin your IDE to use the newly created virtual environment. You can get the python path with:

```shell
hatch run python -c "import sys; print(sys.executable)"
```

- You're good to go! 🎉

### Development process

Please note that you **don't** need to use `hatch` inside notebooks or in the Databricks workspace.
It's only introduced to simplify local development.

Write your code in the IDE. Please keep all relevant files under the `src/uc_migration_toolkit` directory.

Don't forget to test your code via:

```shell
hatch run test
```

Please note that all commits go through the CI process, and it verifies linting. You can run linting locally via:

```shell
hatch run lint:fmt
```
