import ast
import html
import json
import logging
import re
import sys
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


class _ReturnToPrintJsonTransformer(ast.NodeTransformer):
    def __init__(self) -> None:
        self._has_json_import = False
        self.has_return = False

    def apply(self, node: ast.AST) -> ast.AST:
        node = self.visit(node)
        if self.has_return and not self._has_json_import:
            new_import = ast.parse("import json").body[0]
            node.body.insert(0, new_import)
        return node

    def visit_Import(self, node: ast.Import) -> ast.Import:  # noqa: N802
        for name in node.names:
            if "json" != ast.unparse(name):
                continue
            self._has_json_import = True
            break
        return node

    def visit_Return(self, node):  # noqa: N802
        value = node.value
        if not value:
            # Remove the original return statement
            return None
        return_expr = ast.unparse(value)
        replaced_code = f"print(json.dumps({return_expr}))"
        print_call = ast.parse(replaced_code).body[0]
        self.has_return = True
        return print_call


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

        if self._language == Language.PYTHON:
            # perform AST transformations for very repetitive tasks, like JSON serialization
            code_tree = ast.parse(code)
            json_serialize_transform = _ReturnToPrintJsonTransformer()
            new_tree = json_serialize_transform.apply(code_tree)
            code = ast.unparse(new_tree)

        ctx = self._running_command_context()
        result = self._commands.execute(
            cluster_id=self._cluster_id, language=self._language, context_id=ctx.id, command=code
        ).result()

        results = result.results
        if result.status == compute.CommandStatus.FINISHED:
            self._raise_if_failed(results)
            if (
                self._language == Language.PYTHON
                and results.result_type == compute.ResultType.TEXT
                and json_serialize_transform.has_return
            ):
                # parse json from converted return statement
                return json.loads(results.data)
            return results.data
        else:
            # there might be an opportunity to convert builtin exceptions
            raise Exception(results.summary)

    def install_notebook_library(self, library):
        return self.run(
            f"""
            get_ipython().run_line_magic('pip', 'install {library}')
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
            sys.stderr.write(_ascii_escape_re.sub("", results.cause))

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
                if char in (" ", "\t"):
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
