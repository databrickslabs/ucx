import functools
import http.server
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from databricks.labs.blueprint.logger import install_logger
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.base import Diagnostic, Range
from databricks.labs.ucx.code.languages import Languages
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus

logger = logging.getLogger(__name__)


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
    diagnostics: Iterable[Diagnostic]

    def as_dict(self):
        return {"diagnostics": [d.as_dict() for d in self.diagnostics]}


@dataclass
class QuickFixResponse:
    code_actions: Iterable[CodeAction]

    def as_dict(self):
        return {"code_actions": [ca.as_dict() for ca in self.code_actions]}


class Lsp:
    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    def _read(self, file_uri: str):
        file = Path(file_uri.removeprefix("file://"))
        if file.suffix not in self._extensions:
            raise KeyError(f"no language for {file.suffix}")
        language = self._extensions[file.suffix]
        with file.open('r') as f:
            return f.read(), language

    def lint(self, file_uri: str):
        code, language = self._read(file_uri)
        analyser = self._languages.linter(language)
        return AnalyseResponse(list(analyser.lint(code)))

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
    def __init__(self, lsp: Lsp, *args):
        self._lsp = lsp
        super().__init__(*args)

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug(fmt % args)

    def do_POST(self):
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
        response = self._lsp.quickfix(raw['file_uri'], rng, raw['code'])

        raw = json.dumps(response.as_dict()).encode('utf-8')
        self.wfile.write(raw)
        # self.wfile.flush()

    def do_GET(self):
        if not self.path.startswith('/lint'):
            self.send_error(400, 'Wrong input')
            return
        from urllib.parse import parse_qsl

        parts = self.path.split('?')
        if len(parts) != 2:
            self.send_error(400, 'Missing Query')
            return
        path, query_string = parts
        query = dict(parse_qsl(query_string))
        response = self._lsp.lint(query['file_uri'])
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
    languages = Languages(
        Index(
            [
                MigrationStatus(
                    src_schema='old', src_table='things', dst_catalog='brand', dst_schema='new', dst_table='stuff'
                ),
                MigrationStatus(
                    src_schema='other',
                    src_table='matters',
                    dst_catalog='some',
                    dst_schema='certain',
                    dst_table='issues',
                ),
            ]
        )
    )
    lsp = Lsp(languages)
    lsp.serve()
