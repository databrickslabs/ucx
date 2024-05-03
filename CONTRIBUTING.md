# Contributing

## First Principles

We must use the [Databricks SDK for Python](https://databricks-sdk-py.readthedocs.io/) in this project. It is a toolkit for our project. 
If something doesn't naturally belong to the `WorkspaceClient`, it must go through a "mixin" process before it can be used with the SDK.
Imagine the `WorkspaceClient` as the main control center and the "mixin" process as a way to adapt other things to work with it.
You can find an example of how mixins are used with `StatementExecutionExt`. There's a specific example of how to make something 
work with the WorkspaceClient using `StatementExecutionExt`. This example can help you understand how mixins work in practice.

Favoring standard libraries over external dependencies, especially in specific contexts like Databricks, is a best practice in software 
development. 

There are several reasons why this approach is encouraged:
- Standard libraries are typically well-vetted, thoroughly tested, and maintained by the official maintainers of the programming language or platform. This ensures a higher level of stability and reliability. 
- External dependencies, especially lesser-known or unmaintained ones, can introduce bugs, security vulnerabilities, or compatibility issues  that can be challenging to resolve. Adding external dependencies increases the complexity of your codebase. 
- Each dependency may have its own set of dependencies, potentially leading to a complex web of dependencies that can be difficult to manage. This complexity can lead to maintenance challenges, increased risk, and longer build times. 
- External dependencies can pose security risks. If a library or package has known security vulnerabilities and is widely used, it becomes an attractive target for attackers. Minimizing external dependencies reduces the potential attack surface and makes it easier to keep your code secure. 
- Relying on standard libraries enhances code portability. It ensures your code can run on different platforms and environments without being tightly coupled to specific external dependencies. This is particularly important in settings like Databricks, where you may need to run your code on different clusters or setups. 
- External dependencies may have their versioning schemes and compatibility issues. When using standard libraries, you have more control over versioning and can avoid conflicts between different dependencies in your project. 
- Fewer external dependencies mean faster build and deployment times. Downloading, installing, and managing external packages can slow down these processes, especially in large-scale projects or distributed computing environments like Databricks. 
- External dependencies can be abandoned or go unmaintained over time. This can lead to situations where your project relies on outdated or unsupported code. When you depend on standard libraries, you have confidence that the core functionality you rely on will continue to be maintained and improved. 

While minimizing external dependencies is essential, exceptions can be made case-by-case. There are situations where external dependencies are 
justified, such as when a well-established and actively maintained library provides significant benefits, like time savings, performance improvements, 
or specialized functionality unavailable in standard libraries.

## Change management

When you introduce a change in the code, specifically a deeply technical one, please ensure that the change provides same or improved set of capabilities.
PRs that remove existing functionality shall be properly discussed and justified.

## Code Organization

When writing code, divide it into two main parts: **Components for API Interaction** and **Components for Business Logic**.
API Interaction should only deal with talking to external systems through APIs. They are usually integration-tested, and mocks are simpler.
Business Logic handles the actual logic of your application, like calculations, data processing, and decision-making.

_Keep API components simple._ In the components responsible for API interactions, try to keep things as straightforward as possible.
Refrain from overloading them with complex logic; instead, focus on making API calls and handling the data from those calls.

_Inject Business Logic._ If you need to use business logic in your API-calling components, don't build it directly there.
Instead, inject (or pass in) the business logic components into your API components. This way, you can keep your API components 
clean and flexible, while the business logic remains separate and reusable.

_Test your Business Logic._ It's essential to test your business logic to ensure it works correctly and thoroughly. When writing 
unit tests, avoid making actual API calls - unit tests are executed for every pull request, and **_take seconds to complete_**. 
For calling any external services, including Databricks Connect, Databricks Platform, or even Apache Spark, unit tests have 
to use "mocks" or fake versions of the APIs to simulate their behavior. This makes testing your code more manageable and catching any 
issues without relying on external systems. Focus on testing the edge cases of the logic, especially the scenarios where 
things may fail. See [this example](https://github.com/databricks/databricks-sdk-py/pull/295) as a reference of an extensive
unit test coverage suite and the clear difference between _unit tests_ and _integration tests_.

## Common fixes for `mypy` errors

See https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html for more details

### `if typing.TYPE_CHECKING:` usage

We don't use `if typing.TYPE_CHECKING:` in our codebase, because PyCharm and `mypy` can handle it without it and `Refactoring -> Move` may produce incorrect results.

### ..., expression has type "None", variable has type "str"

* Add `assert ... is not None` if it's a body of a method. Example:

```
# error: Argument 1 to "delete" of "DashboardWidgetsAPI" has incompatible type "str | None"; expected "str"
self._ws.dashboard_widgets.delete(widget.id)
```

after

```
assert widget.id is not None
self._ws.dashboard_widgets.delete(widget.id)
```

* Add `... | None` if it's in the dataclass. Example: `cloud: str = None` -> `cloud: str | None = None`

### ..., has incompatible type "Path"; expected "str"

Add `.as_posix()` to convert Path to str

###  Argument 2 to "get" of "dict" has incompatible type "None"; expected ...

Add a valid default value for the dictionary return. 

Example: 
```python
def viz_type(self) -> str:
    return self.viz.get("type", None)
```

after:

Example: 
```python
def viz_type(self) -> str:
    return self.viz.get("type", "UNKNOWN")
```

## Integration Testing Infrastructure

Integration tests must accompany all new code additions. Integration tests help us validate that various parts of 
our application work correctly when they interact with each other or external systems. This practice ensures that our 
software _**functions as a cohesive whole**_. Integration tests run every night and take approximately 15 minutes
for the entire test suite to complete.

We encourage using predefined test infrastructure provided through environment variables for integration tests. 
These fixtures are set up in advance to simulate specific scenarios, making it easier to test different use cases. These 
predefined fixtures enhance test consistency and reliability and point to the real infrastructure used by integration
testing. See [Unified Authentication Documentation](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html)
for the latest reference of environment variables related to authentication.

- `CLOUD_ENV`: This environment variable specifies the cloud environment where Databricks is hosted. The values typically 
  indicate the cloud provider being used, such as "aws" for Amazon Web Services and "azure" for Microsoft Azure.
- `DATABRICKS_ACCOUNT_ID`: This variable stores the unique identifier for your Databricks account.
- `DATABRICKS_HOST`: This variable contains the URL of your Databricks workspace. It is the web address you use to access 
  your Databricks environment and typically looks like "https://dbc-....cloud.databricks.com."
- `TEST_DEFAULT_CLUSTER_ID`: This variable holds the identifier for the default cluster used in testing. The value 
  resembles a unique cluster ID, like "0824-163015-tdtagl1h."
- `TEST_DEFAULT_WAREHOUSE_DATASOURCE_ID`: This environment variable stores the identifier for the default warehouse data 
  source used in testing. The value is a unique identifier for the data source, such as "3c0fef12-ff6c-...".
- `TEST_DEFAULT_WAREHOUSE_ID`: This variable contains the identifier for the default warehouse used in testing. The value 
  resembles a unique warehouse ID, like "49134b80d2...".
- `TEST_INSTANCE_POOL_ID`: This environment variable stores the identifier for the instance pool used in testing. 
  You must utilise existing instance pools as much as possible for cluster startup time and cost reduction. 
  The value is a unique instance pool ID, like "0824-113319-...".
- `TEST_LEGACY_TABLE_ACL_CLUSTER_ID`: This variable holds the identifier for the cluster used in testing legacy table
  access control. The value is a unique cluster ID, like "0824-161440-...".
- `TEST_USER_ISOLATION_CLUSTER_ID`: This environment variable contains the identifier for the cluster used in testing
  user isolation. The value is a unique cluster ID, like "0825-164947-...".

With the final results looking like (Note, for Azure Cloud, no `DATABRICKS_TOKEN` value):
```shell
export DATABRICKS_ACCOUNT_ID=999f9f99-89ba-4dd2-9999-a3301d0f21c0
export DATABRICKS_HOST=https://adb-8590162618999999.14.azuredatabricks.net
export DATABRICKS_CLUSTER_ID=8888-999999-6w1gh5v6
export TEST_DEFAULT_WAREHOUSE_ID=a73e69a521a9c91c
export TEST_DEFAULT_CLUSTER_ID=8888-999999-6w1gh5v6
export TEST_LEGACY_TABLE_ACL_CLUSTER_ID=8888-999999-9fafnezi
export TEST_INSTANCE_POOL_ID=7777-999999-save46-pool-cghrk21s
```

The test workspace must have test user accounts, matching displayName pattern `test-uesr-*`
To create test users:

```shell
databricks users create --active --display-name "test-user-1" --user-name "first.last-t1@example.com"
databricks users create --active --display-name "test-user-2" --user-name "first.last-t2@example.com"
```

Before running integration tests on Azure Cloud, you must log in (and clear any TOKEN authentication):

```shell
az login
unset DATABRICKS_TOKEN
```

Use the following command to run the integration tests:

```shell
make integration
```

### Fixtures
We'd like to encourage you to leverage the extensive set of [pytest fixtures](https://docs.pytest.org/en/latest/explanation/fixtures.html#about-fixtures). 
These fixtures follow a consistent naming pattern, starting with "make_". These functions can be called multiple 
times to _create and clean up objects as needed_ for your tests. Reusing these fixtures helps maintain clean and consistent 
test setups across the codebase. In cases where your tests require unique fixture setups, keeping the wall
clock time of fixture initialization under one second is crucial. Fast fixture initialization ensures that tests run quickly, reducing
development cycle times and allowing for more immediate feedback during development.

```python
from databricks.sdk.service.workspace import AclPermission
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403

def test_secret_scope_acl(make_secret_scope, make_secret_scope_acl, make_group):
    scope_name = make_secret_scope()
    make_secret_scope_acl(scope=scope_name, principal=make_group().display_name, permission=AclPermission.WRITE)
```

If the fixture requires no argument and special cleanup, you can simplify the fixture from 
```python
@pytest.fixture
def make_thing(...):
    def inner():
        ...
        return x
    return inner
```
to:
```python
@pytest.fixture
def thing(...):
    ...
    return x
```

### Debugging
Each integration test _must be debuggable_ within the free [IntelliJ IDEA (Community Edition)](https://www.jetbrains.com/idea/download) 
with the [Python plugin (Community Edition)](https://plugins.jetbrains.com/plugin/7322-python-community-edition). If it works within 
IntelliJ CE, then it would work in PyCharm. Debugging capabilities are essential for troubleshooting and diagnosing issues during 
development. Please make sure that your test setup allows for easy debugging by following best practices.

![debugging tests](docs/debugging-tests.gif)

Adhering to these guidelines ensures that our integration tests are robust, efficient, and easily maintainable. This, 
in turn, contributes to the overall reliability and quality of our software.

Currently, VSCode IDE is not supported, as it does not offer interactive debugging single integration tests. 
However, it's possible that this limitation may be addressed in the future.

### Flaky tests

You can add `@retried` decorator to deal with [flaky tests](https://docs.pytest.org/en/latest/explanation/flaky.html):

```python
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_something(ws):
    ...

```

## Local Setup

This section provides a step-by-step guide to set up and start working on the project. These steps will help you set up your project environment and dependencies for efficient development.

To begin, install [Hatch](https://github.com/pypa/hatch), which is our build tool.

On MacOSX, this is achieved using the following:
```shell
brew install hatch
```

The clone the github repo, and `cd` into it.

To begin, run `make dev` to create the default environment and install development dependencies.

```shell
make dev
```

Configure your IDE to use `.venv/bin/python` from the virtual environment when developing the project:
![IDE Setup](docs/hatch-intellij.gif)


Verify installation with 
```shell
make test
```


Before every commit, apply the consistent styleguide and formatting of the code, as we want our codebase to look consistent. Consult the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html) if you have any doubts. Make sure to run the tests.
```shell
make fmt test
```

## First contribution

Here are the example steps to submit your first contribution:

1. [Make a Fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo) from the repo
2. `git clone`
3. `git checkout main` (or `gcm` if you're using [oh-my-zsh](https://ohmyz.sh/)).
4. `git pull` (or `gl` if you're using [oh-my-zsh](https://ohmyz.sh/)).
5. `git checkout -b FEATURENAME` (or `gcb FEATURENAME` if you're using [oh-my-zsh](https://ohmyz.sh/)).
6. ... do the work
7. `make fmt`
8. ... fix if any
9. `make test`
10. ... fix if any
11. `git commit -a`. Make sure to enter a meaningful commit message title.
12. `git push origin FEATURENAME`
13. Go to GitHub UI and create PR. Alternatively, `gh pr create` (if you have [GitHub CLI](https://cli.github.com/) installed). 
    Use a meaningful pull request title because it'll appear in the release notes. Use `Resolves #NUMBER` in pull
    request description to [automatically link it](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests#linking-a-pull-request-to-an-issue)
    to an existing issue. 
14. Announce PR for the review.

## Troubleshooting

If you encounter any package dependency errors after `git pull`, run `make clean`

### Environment Issues

Sometimes, when dependencies are updated via `dependabot` for example, the environment may report the following error:

```sh
...
ERROR: Cannot install databricks-labs-ucx[test]==0.0.3 and databricks-sdk~=0.8.0 because these package versions have conflicting dependencies.
ERROR: ResolutionImpossible: for help visit https://pip.pypa.io/en/latest/topics/dependency-resolution/#dealing-with-dependency-conflicts
```

The easiest fix is to remove the environment and have the re-run recreate it:

```sh
$ hatch env show
                                 Standalone                                 
┏━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Name        ┃ Type    ┃ Dependencies                   ┃ Scripts         ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ default     │ virtual │                                │                 │
├─────────────┼─────────┼────────────────────────────────┼─────────────────┤
│ unit        │ virtual │ databricks-labs-ucx[test]      │ test            │
│             │         │ delta-spark<3.0.0,>=2.4.0      │ test-cov-report │
│             │         │ pyspark<=3.5.0,>=3.4.0         │                 │
├─────────────┼─────────┼────────────────────────────────┼─────────────────┤
│ integration │ virtual │ databricks-labs-ucx[dbconnect] │ test            │
│             │         │ databricks-labs-ucx[test]      │                 │
│             │         │ delta-spark<3.0.0,>=2.4.0      │                 │
├─────────────┼─────────┼────────────────────────────────┼─────────────────┤
│ lint        │ virtual │ black>=23.1.0                  │ fmt             │
│             │         │ isort>=2.5.0                   │ verify          │
│             │         │ ruff>=0.0.243                  │                 │
└─────────────┴─────────┴────────────────────────────────┴─────────────────┘

$ make clean dev test
========================================================================================== test session starts ===========================================================================================
platform darwin -- Python 3.11.4, pytest-7.4.1, pluggy-1.3.0 -- /Users/lars.george/Library/Application Support/hatch/env/virtual/databricks-labs-ucx/H6b8Oom-/unit/bin/python
cachedir: .pytest_cache
rootdir: /Users/lars.george/projects/work/databricks/ucx
configfile: pyproject.toml
plugins: cov-4.1.0, mock-3.11.1
collected 103 items                                                                                                                                                                                      

tests/unit/test_config.py::test_initialization PASSED
tests/unit/test_config.py::test_reader PASSED
...
tests/unit/test_tables.py::test_uc_sql[table1-CREATE VIEW IF NOT EXISTS new_catalog.db.view AS SELECT * FROM table;] PASSED
tests/unit/test_tables.py::test_uc_sql[table2-CREATE TABLE IF NOT EXISTS new_catalog.db.external_table LIKE catalog.db.external_table COPY LOCATION;ALTER TABLE catalog.db.external_table SET TBLPROPERTIES ('upgraded_to' = 'new_catalog.db.external_table');] PASSED

---------- coverage: platform darwin, python 3.11.4-final-0 ----------
Coverage HTML written to dir htmlcov

========================================================================================== 103 passed in 12.61s ==========================================================================================
$ 
```

Sometimes, when multiple Python versions are installed in the workstation used for UCX development, one might encounter the following:

```sh
$ make dev
ERROR: Environment `default` is incompatible: cannot locate Python: 3.10
```

The easiest fix is to reinstall Python 3.10. If required remove the installed hatch package manager and reinstall it:

```sh
$ deactivate
$ brew install python@3.10
$ rm -rf venv
$ pip3 uninstall hatch
$ python3.10 -m pip install hatch
$ make dev
$ make test
```
Note: Before performing a clean installation, deactivate the virtual environment and follow the commands given above.

Note: The initial `hatch env show` is just to list the environments managed by Hatch and is not needed.
