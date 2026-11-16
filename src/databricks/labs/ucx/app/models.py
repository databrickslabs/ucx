from pydantic import BaseModel, Field, model_validator

from databricks.labs.ucx.hive_metastore.mapping import Rule


class WorkflowStatus(BaseModel):
    step: str
    state: str
    started: str


class Status(BaseModel):
    host: str
    user: str
    auth_mode: str
    install_folder: str
    inventory_database: str
    ucx_version: str
    workflows: list[WorkflowStatus]


class RunAssessmentRequest(BaseModel):
    force_refresh: bool = False


class WorkflowRun(BaseModel):
    workflow: str
    run_id: int
    run_url: str


class ActiveRun(BaseModel):
    """A UCX run. ``state`` is the life cycle state while it runs (e.g. RUNNING) and the result once it finished."""

    name: str  # the UCX workflow, or the name of a chained migration run started from the app
    run_id: int
    state: str
    state_message: str = ""
    started_at: str | None = None  # ISO 8601, UTC
    run_url: str


class ActiveRuns(BaseModel):
    runs: list[ActiveRun]


class Dashboard(BaseModel):
    dashboard_id: str
    url: str
    embed_url: str


class MappingRule(BaseModel):
    """API representation of a :class:`Rule`, i.e. one row of ``mapping.csv``."""

    workspace_name: str = Field(min_length=1)
    catalog_name: str = Field(min_length=1)
    src_schema: str = Field(min_length=1)
    dst_schema: str = Field(min_length=1)
    src_table: str = Field(min_length=1)
    dst_table: str = Field(min_length=1)
    recon_tolerance_percent: int = Field(default=0, ge=0, le=100)
    compare_rows: bool = False

    @classmethod
    def from_rule(cls, rule: Rule) -> "MappingRule":
        return cls(**rule.__dict__)

    def to_rule(self) -> Rule:
        return Rule(**self.model_dump())


class Mapping(BaseModel):
    rules: list[MappingRule]


class MigrationPrecheck(BaseModel):
    total_tables: int
    hiveserde_tables: int
    external_ctas_tables: int
    prerequisites_error: str | None = None  # set when the migration prerequisites are not met


class RunMigrationRequest(BaseModel):
    """The migration workflows to run. The selected workflows run one after the other, in the order of the fields."""

    migrate_tables: bool = True
    hiveserde_in_place: bool = False
    external_ctas: bool = False

    @model_validator(mode="after")
    def _at_least_one(self) -> "RunMigrationRequest":
        if not (self.migrate_tables or self.hiveserde_in_place or self.external_ctas):
            raise ValueError("Select at least one migration workflow")
        return self


class MigrationRun(BaseModel):
    """A one-time job run that runs the selected UCX migration workflows sequentially."""

    run_id: int
    run_url: str
    workflows: list[str]
