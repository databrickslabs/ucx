import contextlib
import functools
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread

import yaml

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task
from databricks.labs.ucx.runtime import Workflows


@contextlib.contextmanager
def http_fixture_server(handler: Callable[[BaseHTTPRequestHandler], None]):

    class _handler(BaseHTTPRequestHandler):
        def __init__(self, handler: Callable[[BaseHTTPRequestHandler], None], *args):
            self._handler = handler
            super().__init__(*args)

        def __getattr__(self, item):
            if item[0:3] != "do_":
                raise AttributeError(f"method {item} not found")
            return functools.partial(self._handler, self)

    handler_factory = functools.partial(_handler, handler)
    srv = HTTPServer(("localhost", 0), handler_factory)
    srv_thread = Thread(target=srv.serve_forever)
    try:
        srv_thread.daemon = True
        srv_thread.start()
        yield "http://{0}:{1}".format(*srv.server_address)  # pylint: disable=consider-using-f-string
    finally:
        srv.shutdown()


def test_user_agent_is_propagated(tmp_path):
    user_agent = {}

    def inner(handler: BaseHTTPRequestHandler):
        for pair in handler.headers["User-Agent"].split(" "):
            if "/" not in pair:
                continue
            key, value = pair.split("/")
            user_agent[key] = value
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(b"{}")
        handler.wfile.flush()

    class Dummy(Workflow):
        def __init__(self):
            super().__init__('dummy')

        @job_task
        def something(self, ctx: RuntimeContext):
            """Some comment"""
            ctx.workspace_client.current_user.me()

    workflows = Workflows([Dummy()])
    with http_fixture_server(inner) as host:
        cfg = Path(tmp_path, "config.yml")
        cfg.write_text(
            yaml.safe_dump(
                {
                    'version': 2,
                    'inventory_database': '_',
                    'connect': {"host": host, "token": "_"},
                }
            ),
            encoding="utf-8",
        )
        workflows.trigger(f"--config={cfg.as_posix()}", "--workflow=dummy", "--task=something")

    assert "ucx" in user_agent
    assert "cmd" in user_agent
    assert user_agent["ucx"] == __version__
    assert user_agent["cmd"] == "dummy"
