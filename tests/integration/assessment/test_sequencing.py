def test_migration_sequencing_simple_job(make_job, runtime_ctx) -> None:
    """Sequence a simple job"""
    job = make_job()

    maybe_job_node = runtime_ctx.migration_sequencer.register_job(job)
    assert not maybe_job_node.failed

    steps = runtime_ctx.migration_sequencer.generate_steps()
    step_object_types = [step.object_type for step in steps]
    assert step_object_types == ["TASK", "JOB"]
