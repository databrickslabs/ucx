from dataclasses import dataclass
import datetime as dt


@dataclass
class HistoryRecord:
    workspace_id: int  # The workspace id
    run_id: int  # The workflow run id that crawled the objects
    run_start_time: dt.datetime  # The workflow run timestamp that crawled the objects
    object_type: str  # The object type, e.g. TABLE, VIEW. Forms a composite key together with object_id
    object_id: str  # The object id, e.g. hive_metastore.database.table. Forms a composite key together with object_id
    object_data: str  # The object data; the attributes of the corresponding ucx data class, e.g. table name, table ...
    failures: list  # The failures indicating the object is not UC compatible
    owner: str  # The object owner
    ucx_version: str  # The ucx semantic version
    snapshot_id: int  # An identifier for the snapshot

