import enum
import functools
import http.server
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from databricks.labs.blueprint.logger import install_logger
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableMigrationStatus
from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Convention,
    Deprecation,
    Failure,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext

logger = logging.getLogger(__name__)


@dataclass
class Position:
    # LSP uses 0-based line and character positions.
    line: int
    character: int

    def as_dict(self) -> dict:
        return {"line": self.line, "character": self.character}

    @classmethod
    def from_dict(cls, raw: dict) -> 'Position':
        return cls(raw['line'], raw['character'])


@dataclass
class Range:
    start: Position
    end: Position

    @classmethod
    def from_dict(cls, raw: dict) -> 'Range':
        return cls(Position.from_dict(raw['start']), Position.from_dict(raw['end']))

    @classmethod
    def make(cls, start_line: int, start_character: int, end_line: int, end_character: int) -> 'Range':
        return cls(start=Position(start_line, start_character), end=Position(end_line, end_character))

    def as_dict(self) -> dict:
        return {"start": self.start.as_dict(), "end": self.end.as_dict()}

    def fragment(self, code: str) -> str:
        out = []
        splitlines = code.splitlines()
        for line, part in enumerate(splitlines):
            if line == self.start.line and line == self.end.line:
                out.append(part[self.start.character : self.end.character])
            elif line == self.start.line:
                out.append(part[self.start.character :])
            elif line == self.end.line:
                out.append(part[: self.end.character])
            elif self.start.line < line < self.end.line:
                out.append(part)
        return "".join(out)


class Severity(enum.IntEnum):
    ERROR = 1
    WARN = 2
    INFO = 3
    HINT = 4


class DiagnosticTag(enum.IntEnum):
    UNNECESSARY = 1
    DEPRECATED = 2


@dataclass
class Diagnostic:
    # the range at which the message applies.
    range: Range

    # The diagnostic's code, which might appear in the user interface.
    code: str

    # An optional property to describe the error code.
    source: str

    # The diagnostic's message.
    message: str

    # The diagnostic's severity. Can be omitted. If omitted it is up to the
    # client to interpret diagnostics as error, warning, info or hint.
    severity: Severity

    tags: list[DiagnosticTag] | None = None

    @classmethod
    def from_advice(cls, advice: Advice) -> 'Diagnostic':
        severity, tags = cls._severity_and_tags(advice)
        return cls(
            range=Range.make(advice.start_line, advice.start_col, advice.end_line, advice.end_col),
            code=advice.code,
            source="databricks.labs.ucx",
            message=advice.message,
            severity=severity,
            tags=tags,
        )

    @classmethod
    def _severity_and_tags(cls, advice):
        if isinstance(advice, Convention):
            return Severity.HINT, [DiagnosticTag.UNNECESSARY]
        if isinstance(advice, Deprecation):
            return Severity.WARN, [DiagnosticTag.DEPRECATED]
        if isinstance(advice, Advisory):
            return Severity.WARN, []
        if isinstance(advice, Failure):
            return Severity.ERROR, []
        return Severity.INFO, []

    def as_dict(self) -> dict:
        return {
            "range": self.range.as_dict(),
            "code": self.code,
            "source": self.source,
            "message": self.message,
            "severity": self.severity.value if self.severity else Severity.WARN,
            "tags": [t.value for t in self.tags] if self.tags else [],
        }


@dataclass
class TextDocumentIdentifier:
    uri: str

    def as_dict(self) -> dict:
        return {
            "uri": self.uri,
        }


@dataclass
class OptionalVersionedTextDocumentIdentifier:
    uri: str
    version: int | None = None

    def as_dict(self) -> dict:
        return {
            "uri": self.uri,
            "version": self.version,
        }


@dataclass
class TextEdit:
    range: Range
    new_text: str

    def as_dict(self) -> dict:
        return {
            "range": self.range.as_dict(),
            "newText": self.new_text,
        }


@dataclass
class TextDocumentEdit:
    text_document: OptionalVersionedTextDocumentIdentifier
    edits: Sequence[TextEdit]

    def as_dict(self) -> dict:
        return {
            "textDocument": self.text_document.as_dict(),
            "edits": [e.as_dict() for e in self.edits],
        }


@dataclass
class WorkspaceEdit:
    # we also can have CreateFile | RenameFile | DeleteFile, but we won't do it for now.
    document_changes: Sequence[TextDocumentEdit]

    def as_dict(self) -> dict:
        return {
            "documentChanges": [e.as_dict() for e in self.document_changes],
        }


@dataclass
class CodeAction:
    title: str
    edit: WorkspaceEdit
    is_preferred: bool = False

    def as_dict(self) -> dict:
        return {
            "title": self.title,
            "edit": self.edit.as_dict(),
            "isPreferred": self.is_preferred,
        }


@dataclass
class AnalyseResponse:
    diagnostics: list[Diagnostic]

    def as_dict(self):
        return {"diagnostics": [d.as_dict() for d in self.diagnostics]}


@dataclass
class QuickFixResponse:
    code_actions: list[CodeAction]

    def as_dict(self):
        return {"code_actions": [ca.as_dict() for ca in self.code_actions]}


class LspServer:
    def __init__(self, language_support: LinterContext):
        self._languages = language_support
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    def _read(self, file_uri: str):
        file = Path(file_uri.removeprefix("file://"))
        if file.suffix not in self._extensions:
            raise KeyError(f"no language for {file.suffix}")
        language = self._extensions[file.suffix]
        with file.open('r', encoding='utf8') as f:
            return f.read(), language

    def lint(self, file_uri: str):
        code, language = self._read(file_uri)
        analyser = self._languages.linter(language)
        diagnostics = [Diagnostic.from_advice(_) for _ in analyser.lint(code)]
        return AnalyseResponse(diagnostics)

    def quickfix(self, file_uri: str, code_range: Range, diagnostic_code: str):
        code, language = self._read(file_uri)
        fixer = self._languages.fixer(language, diagnostic_code)
        if not fixer:
            return QuickFixResponse(code_actions=[])
        fragment = code_range.fragment(code)
        apply = fixer.apply(fragment)
        return QuickFixResponse(
            code_actions=[
                CodeAction(
                    title=f"Replace with: {apply}",
                    edit=WorkspaceEdit(
                        document_changes=[
                            TextDocumentEdit(
                                text_document=OptionalVersionedTextDocumentIdentifier(file_uri),
                                edits=[TextEdit(code_range, apply)],
                            ),
                        ]
                    ),
                    is_preferred=True,
                )
            ]
        )

    def serve(self):
        server_address = ('localhost', 8000)
        handler_class = functools.partial(_RequestHandler, self)
        httpd = http.server.ThreadingHTTPServer(server_address, handler_class)
        httpd.serve_forever()


class _RequestHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, lsp_server: LspServer, *args):
        self._lsp_server = lsp_server
        super().__init__(*args)

    def log_message(self, fmt: str, *args: Any):  # pylint: disable=arguments-differ
        logger.debug(fmt % args)  # pylint: disable=logging-not-lazy

    def do_POST(self):  # pylint: disable=invalid-name
        if not self.path.startswith('/quickfix'):
            self.send_error(400, 'Wrong input')
            return
        self.send_response(200)
        self.end_headers()

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        raw = json.loads(post_data.decode('utf-8'))
        logger.debug(f"Received:\n{raw}")

        rng = Range.from_dict(raw['range'])
        response = self._lsp_server.quickfix(raw['file_uri'], rng, raw['code'])

        raw = json.dumps(response.as_dict()).encode('utf-8')
        self.wfile.write(raw)

    def do_GET(self):  # pylint: disable=invalid-name
        if not self.path.startswith('/lint'):
            self.send_error(400, 'Wrong input')
            return
        parts = self.path.split('?')
        if len(parts) != 2:
            self.send_error(400, 'Missing Query')
            return
        _, query_string = parts
        query = dict(parse_qsl(query_string))
        response = self._lsp_server.lint(query['file_uri'])
        if not response:
            self.send_error(404, 'no analyser for file type')
            return

        self.send_response(200)
        self.end_headers()
        raw = json.dumps(response.as_dict()).encode('utf-8')
        self.wfile.write(raw)
        self.wfile.flush()


if __name__ == '__main__':
    install_logger()
    logging.root.setLevel('DEBUG')
    languages = LinterContext(
        TableMigrationIndex(
            [
                TableMigrationStatus(
                    src_schema='old', src_table='things', dst_catalog='brand', dst_schema='new', dst_table='stuff'
                ),
                TableMigrationStatus(
                    src_schema='other',
                    src_table='matters',
                    dst_catalog='some',
                    dst_schema='certain',
                    dst_table='issues',
                ),
            ]
        )
    )
    lsp = LspServer(languages)
    lsp.serve()
