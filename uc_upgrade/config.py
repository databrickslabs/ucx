# represents classes and utilities relevant for the configuration patterns.
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from databricks.sdk.runtime import *  # noqa: F403


@dataclass
class AuthConfig:
    token: str
    workspace_host: str
    account_console_host: str

    @property
    def _notebook_context(self) -> Any:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F405

    @classmethod
    def from_runtime(cls) -> AuthConfig:
        _token = cls._notebook_context.apiUrl().getOrElse(None)
        _host = cls._notebook_context.apiToken().getOrElse(None)
        return AuthConfig(token=_token, workspace_host=_host, account_console_host="")


@dataclass
class MigrationConfig:
    pass


@dataclass
class Config:
    # main configuration class, responsible for combining various parts of the task
    auth: AuthConfig
    migration: MigrationConfig
