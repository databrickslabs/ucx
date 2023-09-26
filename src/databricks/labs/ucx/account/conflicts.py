import collections
import logging

from databricks.labs.ucx.account.workspaces import Workspaces
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.workspace_access.groups import GroupCrawler

logger = logging.getLogger(__name__)


def groups(workspaces: Workspaces):
    workspace_members = {}
    workspace_names = {}
    for workspace in workspaces.configured_workspaces():
        workspace_names[workspace.workspace_id] = workspace.workspace_name
        ws = workspaces.client_for(workspace)
        group_crawler = GroupCrawler(ws)
        logger.debug(f"scanning users from {workspace.workspace_name}")
        workspace_members[workspace.workspace_id] = group_crawler.membership()
    seen = set()
    no_conflicts = set()
    for workspace_id, membership in workspace_members.items():
        for group_name, usernames in membership.items():
            if group_name in seen:
                continue
            diffs = {}
            # this is O((n^3)/2), but maybe there's a better algo
            for other_workspace_id in workspace_members.keys():
                if workspace_id == other_workspace_id:
                    continue  # self-link
                other_usernames = workspace_members[other_workspace_id][group_name]
                diff_a = usernames.difference(other_usernames)
                diff_b = other_usernames.difference(usernames)
                # record membership differences between workspace groups
                diffs[other_workspace_id] = (diff_a, diff_b)
            no_diff_counter = 0
            for other_workspace_id, (diff_a, diff_b) in diffs.items():
                if len(diff_a) == 0 and len(diff_b) == 0:
                    no_diff_counter += 1
                    continue
                workspace_name = workspace_names[workspace_id]
                other_workspace_name = workspace_names[other_workspace_id]
                msg = []
                if diff_a:
                    msg.append(
                        f'{workspace_name} has {len(diff_a)} more users than '
                        f'{other_workspace_name}: {", ".join(diff_a)}'
                    )
                if diff_b:
                    msg.append(
                        f'{other_workspace_name} has {len(diff_b)} more users than '
                        f'{workspace_name}: {", ".join(diff_b)}'
                    )
                logger.warning(f'[{group_name}] Inconsistency: {". ".join(msg)}')
            if no_diff_counter == len(diffs):
                no_conflicts.add(group_name)
            seen.add(group_name)
    return no_conflicts


def tables(workspaces: Workspaces):
    workspace_tables = {}
    workspace_names = {}
    for workspace in workspaces.configured_workspaces():
        workspace_names[workspace.workspace_id] = workspace.workspace_name
        ws = workspaces.client_for(workspace)
        me = ws.current_user.me()

        # TODO: catch exceptions for the workspaces, where we don't have UCX installed
        raw_config = ws.workspace.download(f"/Users/{me.user_name}/.ucx/config.yml")
        workspace_config = WorkspaceConfig.from_bytes(raw_config)
        backend = StatementExecutionBackend(ws, workspace_config.warehouse_id)

        logger.debug(f"retrieving crawled tables from {workspace.workspace_name}")
        tables_crawler = TablesCrawler(backend, workspace_config.inventory_database)
        workspace_tables[workspace.workspace_id] = tables_crawler.snapshot()

    tables_on_workspaces = collections.defaultdict(set)
    metadata = collections.defaultdict(set)
    for workspace_id, found_tables in workspace_tables.items():
        for table in found_tables:
            # holds table names based on workspace ids
            tables_on_workspaces[table.key].add(workspace_id)
            # TODO: check if we have to reformat/normalise the view definition queries
            metadata[table.key].add((table.object_type, table.table_format, table.location, table.view_text))

    no_conflict_databases = set()
    conflicting_databases = set()
    for full_name, properties_set in metadata.items():
        _, database, table_name = full_name.split(".")
        if len(properties_set) == 1:
            no_conflict_databases.add(database)
            continue
        conflicting_databases.add(database)

        # create columns to compute what is actually different
        object_type_set = set()
        table_format_set = set()
        location_set = set()
        view_text_set = set()
        for object_type, table_format, location, view_text in properties_set:
            object_type_set.add(object_type)
            table_format_set.add(table_format)
            location_set.add(location)
            view_text_set.add(view_text)

        # report conflicting definitions back to the user
        names = " and ".join(workspace_names[_] for _ in tables_on_workspaces[full_name])
        msg = [f"Conflicting definitions of {full_name} on {names} workspaces"]
        for what, column in (
            ("object_type", object_type_set),
            ("table_format", table_format_set),
            ("location", location_set),
            ("view_text", view_text_set),
        ):
            if len(column) == 1:
                continue
            msg.append(f'{what} difference: {" and ".join(column)}')

        logger.warning(", ".join(msg))

    for conflict in conflicting_databases:
        no_conflict_databases.remove(conflict)

    return no_conflict_databases
