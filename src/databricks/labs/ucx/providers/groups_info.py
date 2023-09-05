from dataclasses import dataclass

from databricks.sdk.service.iam import Group


@dataclass
class MigrationGroupInfo:
    workspace: Group
    backup: Group
    account: Group


class GroupMigrationState:
    """Holds migration state of workspace-to-account groups"""

    def __init__(self):
        self.groups: list[MigrationGroupInfo] = []

    def add(self, group: MigrationGroupInfo):
        self.groups.append(group)

    def is_in_scope(self, attr: str, group: Group) -> bool:
        for info in self.groups:
            if getattr(info, attr).id == group.id:
                return True
        return False

    def get_by_workspace_group_name(self, workspace_group_name: str) -> MigrationGroupInfo | None:
        found = [g for g in self.groups if g.workspace.display_name == workspace_group_name]
        if len(found) == 0:
            return None
        else:
            return found[0]
