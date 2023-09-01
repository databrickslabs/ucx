import sys

from databricks.sdk import WorkspaceClient


def main():
    ws = WorkspaceClient(auth_type="runtime")
    print(f"run as {ws.current_user.me()}")
    print(f"Invoked from job: {sys.argv}")
