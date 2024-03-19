import functools
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.base import Diagnostic
from databricks.labs.ucx.code.languages import Languages
from databricks.labs.ucx.hive_metastore.table_migrate import Index, MigrationStatus

import http.server

logger = logging.getLogger(__name__)

@dataclass
class AnalyseResponse:
    diagnostics: Iterable[Diagnostic]

    def as_dict(self):
        return {"diagnostics": [d.as_dict() for d in self.diagnostics]}

class Lsp:
    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    def analyse(self, file_uri: str):
        file = Path(file_uri.removeprefix("file://"))
        if file.suffix not in self._extensions:
            return
        language = self._extensions[file.suffix]
        analyser = self._languages.analyser(language)
        with file.open('r') as f:
            code = f.read()
            return AnalyseResponse(analyser.lint(code))

    def serve(self):
        server_address = ('', 8000)
        handler_class = functools.partial(_RequestHandler, self)
        httpd = http.server.ThreadingHTTPServer(server_address, handler_class)
        httpd.serve_forever()


class _RequestHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, lsp: Lsp, *args):
        self._lsp = lsp
        super().__init__(*args)

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug(fmt, *args)

    def do_GET(self):
        from urllib.parse import parse_qsl
        parts = self.path.split('?')
        if len(parts) != 2:
            self.send_error(400, 'Missing Query')
            return

        query = dict(parse_qsl(parts[1]))
        response = self._lsp.analyse(query['file_uri'])
        if not response:
            self.send_error(404, 'no analyser for file type')
            return

        self.send_response(200)
        self.end_headers()
        raw=json.dumps(response.as_dict()).encode('utf-8')
        self.wfile.write(raw)
        self.wfile.flush()


if __name__ == '__main__':
    languages = Languages(Index(
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
    ))
    lsp = Lsp(languages)
    lsp.serve()
