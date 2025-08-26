from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import RunResultState
from databricks.sdk.service.workspace import ExportFormat, ImportFormat


def test_cli_export_xlsx_results(ws, env_or_skip):
    """Integration test for exporting xlsx results."""
    cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    dummy_notebook = """# Databricks notebook source
    # MAGIC
    # COMMAND ----------
    # MAGIC %pip install xlsxwriter -qqq
    # MAGIC dbutils.library.restartPython()
    # COMMAND ----------
    import xlsxwriter
    with xlsxwriter.Workbook("ucx_assessment_main.xlsx") as workbook:
            # Create dummy export file
            worksheet = workbook.add_worksheet()
    """

    directory = "/tmp/ucx"
    notebook = "EXPORT_ASSESSMENT_TO_EXCEL"

    ws.workspace.mkdirs(directory)
    ws.workspace.upload(
        f"{directory}/{notebook}.py", dummy_notebook.encode("utf8"), format=ImportFormat.AUTO, overwrite=True
    )

    export_file_name = "ucx_assessment_main.xlsx"

    run = ws.jobs.submit_and_wait(
        run_name="export-assessment-to-excel-experimental",
        tasks=[
            jobs.SubmitTask(
                notebook_task=jobs.NotebookTask(notebook_path=f"{directory}/{notebook}"),
                task_key="export-assessment",
                existing_cluster_id=cluster_id,
            )
        ],
    )

    expected_excel_signature = b'\x50\x4B\x03\x04'

    if run.state and run.state.result_state == RunResultState.SUCCESS:
        binary_resp = ws.workspace.download(path=export_file_name, format=ExportFormat.SOURCE)
        assert binary_resp.startswith(expected_excel_signature)
