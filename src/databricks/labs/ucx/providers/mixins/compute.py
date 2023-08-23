import html
import logging
import re
import threading

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute
from databricks.sdk.service.compute import Language

_out_re = re.compile(r"Out\[[\d\s]+]:\s")
_tag_re = re.compile(r"<[^>]*>")
_exception_re = re.compile(r".*Exception:\s+(.*)")
_execution_error_re = re.compile(r"ExecutionError: ([\s\S]*)\n(StatusCode=[0-9]*)\n(StatusDescription=.*)\n")
_error_message_re = re.compile(r"ErrorMessage=(.+)\n")
_ascii_escape_re = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]")
_LOG = logging.getLogger("databricks.sdk")


class CommandExecutor:
    def __init__(self, ws: WorkspaceClient, cluster_id: str | None = None, language: Language = Language.PYTHON):
        if not cluster_id:
            cluster_id = ws.config.cluster_id
        if not cluster_id:
            message = "cluster_id is required either in configuration or as an argument"
            raise ValueError(ws.config.wrap_debug_info(message))
        # TODO: this is taken from dbutils implementation, refactor it once this is merged to SDK
        self._cluster_id = cluster_id
        self._language = language
        self._clusters = ws.clusters
        self._commands = ws.command_execution
        self._lock = threading.Lock()
        self._ctx = None

    def run(self, code):
        code = self._trim_leading_whitespace(code)
        ctx = self._running_command_context()
        result = self._commands.execute(
            cluster_id=self._cluster_id, language=compute.Language.PYTHON, context_id=ctx.id, command=code
        ).result()
        if result.status == compute.CommandStatus.FINISHED:
            self._raise_if_failed(result.results)
            return result.results.data
        else:
            raise Exception(result.results.summary)

    def install_notebook_library(self, library):
        return self.run(
            f"""
        %pip install {library}
        dbutils.library.restartPython()
        """
        )

    def _running_command_context(self) -> compute.ContextStatusResponse:
        if self._ctx:
            return self._ctx
        with self._lock:
            if self._ctx:
                return self._ctx
            self._clusters.ensure_cluster_is_running(self._cluster_id)
            self._ctx = self._commands.create(cluster_id=self._cluster_id, language=self._language).result()
        return self._ctx

    def _is_failed(self, results: compute.Results) -> bool:
        return results.result_type == compute.ResultType.ERROR

    def _text(self, results: compute.Results) -> str:
        if results.result_type != compute.ResultType.TEXT:
            return ""
        return _out_re.sub("", str(results.data))

    def _raise_if_failed(self, results: compute.Results):
        if not self._is_failed(results):
            return
        raise DatabricksError(self._error_from_results(results))

    def _error_from_results(self, results: compute.Results):
        if not self._is_failed(results):
            return
        if results.cause:
            _LOG.debug(f'{_ascii_escape_re.sub("", results.cause)}')

        summary = _tag_re.sub("", results.summary)
        summary = html.unescape(summary)

        exception_matches = _exception_re.findall(summary)
        if len(exception_matches) == 1:
            summary = exception_matches[0].replace("; nested exception is:", "")
            summary = summary.rstrip(" ")
            return summary

        execution_error_matches = _execution_error_re.findall(results.cause)
        if len(execution_error_matches) == 1:
            return "\n".join(execution_error_matches[0])

        error_message_matches = _error_message_re.findall(results.cause)
        if len(error_message_matches) == 1:
            return error_message_matches[0]

        return summary

    @staticmethod
    def _trim_leading_whitespace(command_str: str) -> str:
        """Removes leading whitespace, so that Python code blocks that
        are embedded into Python code still could be interpreted properly."""
        lines = command_str.replace("\t", "    ").split("\n")
        leading_whitespace = float("inf")
        if lines[0] == "":
            lines = lines[1:]
        for line in lines:
            pos = 0
            for char in line:
                if char == " " or char == "\t":
                    pos += 1
                else:
                    break
            if pos < leading_whitespace:
                leading_whitespace = pos
        new_command = ""
        for line in lines:
            if line == "" or line.strip(" \t") == "":
                continue
            if len(line) < leading_whitespace:
                new_command += line + "\n"
            else:
                new_command += line[leading_whitespace:] + "\n"
        return new_command
