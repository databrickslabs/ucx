from databricks.sdk.service import jobs


def test_migration_sequencing_simple_job(make_job, runtime_ctx) -> None:
    """Sequence a simple job"""
    job = make_job()

    maybe_job_node = runtime_ctx.migration_sequencer.register_job(job)
    assert not maybe_job_node.failed

    steps = runtime_ctx.migration_sequencer.generate_steps()
    step_object_types = [step.object_type for step in steps]
    assert step_object_types == ["TASK", "JOB"]


def test_migration_sequencing_job_with_task_referencing_cluster(
    make_job,
    make_notebook,
    runtime_ctx,
    env_or_skip,
) -> None:
    """Sequence a job with a task"""
    cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    task = jobs.Task(
        task_key="test-task",
        existing_cluster_id=cluster_id,
        notebook_task=jobs.NotebookTask(notebook_path=str(make_notebook())),
    )
    job = make_job(tasks=[task])

    maybe_job_node = runtime_ctx.migration_sequencer.register_job(job)
    assert not maybe_job_node.failed

    steps = runtime_ctx.migration_sequencer.generate_steps()
    step_object_types = [step.object_type for step in steps]
    assert step_object_types == ["CLUSTER", "TASK", "JOB"]


def test_migration_sequencing_job_with_task_referencing_non_existing_cluster(runtime_ctx) -> None:
    """Sequence a job with a task referencing existing cluster"""
    # Cannot make an actual job referencing a non-exsting cluster
    task = jobs.Task(task_key="test-task", existing_cluster_id="non-existing-id")
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    maybe_job_node = runtime_ctx.migration_sequencer.register_job(job)
    assert maybe_job_node.failed

    steps = runtime_ctx.migration_sequencer.generate_steps()
    step_object_types = [step.object_type for step in steps]
    assert step_object_types == ["CLUSTER", "TASK", "JOB"]  # TODO: What do we expect?
