from dataclasses import replace

from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical


class GrantsProgressEncoder(ProgressEncoder[Grant]):
    """Encoder class:Grant to class:History.

    A failure for a grants implies it cannot be mapped to Unity Catalog.
    """

    def _encode_record_as_historical(self, record: Grant) -> Historical:
        historical = super()._encode_record_as_historical(record)
        failures = []
        if not record.uc_grant_sql():
            type_, key = record.type_and_key()
            failures = [
                f"Hive metastore grant '{record.action_type}' on {type_} {key} "
                "cannot be mapped to a Unity Catalog grant."
            ]
        return replace(historical, failures=historical.failures + failures)
