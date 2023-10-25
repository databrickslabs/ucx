import logging
import sys, json
import webbrowser

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.install import WorkspaceInstaller


def jobs():
    ws = WorkspaceClient()
    installer = WorkspaceInstaller(ws)
    for step, job_id in installer.deployed_steps().items():
        print(step, job_id)


def open_remote_config():
    ws = WorkspaceClient()
    installer = WorkspaceInstaller(ws)

    ws_file_url = installer.notebook_link(installer.config_file)
    webbrowser.open(ws_file_url)


def me():
    ws = WorkspaceClient()
    my_user = ws.current_user.me()
    greeting = input("How to greet you? ")
    print(f'{greeting}, {my_user.user_name}!')


MAPPING = {
    'open-remote-config': open_remote_config,
    'jobs': jobs,
    'me': me,
}


def main(raw):
    payload = json.loads(raw)
    command = payload['command']
    if command not in MAPPING:
        raise KeyError(f'cannot find command: {command}')
    flags = payload['flags']
    log_level = flags.pop('log_level')
    if log_level != 'disabled':
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())

    kwargs = {k.replace('-', '_'): v for k,v in flags.items()}
    MAPPING[command](**kwargs)


if __name__ == "__main__":
    main(*sys.argv[1:])
