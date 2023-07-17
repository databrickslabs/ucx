# represents classes and utilities relevant for the configuration patterns.
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AuthConfig:
    token: str
    workspace_host: str
    account_console_host: str

    @classmethod
    def from_string(cls, token: str):
        pass


@dataclass
class MigrationConfig:
    pass


@dataclass
class Config:
    # main configuration class, responsible for combining various parts of the task
    auth: AuthConfig
    migration: MigrationConfig
