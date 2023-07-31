from dataclasses import dataclass

from databricks.sdk.service.iam import Group


@dataclass
class MigrationGroupInfo:
    workspace: Group
    backup: Group
    account: Group


class MigrationGroupsProvider:
    def __init__(self):
        self.groups: list[MigrationGroupInfo] = []

    def add(self, group: MigrationGroupInfo):
        self.groups.append(group)

    def get_by_workspace_group_name(self, workspace_group_name: str) -> MigrationGroupInfo | None:
        found = [g for g in self.groups if g.workspace.display_name == workspace_group_name]
        if len(found) == 0:
            return None
        else:
            return found[0]
