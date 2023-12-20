import datetime
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import AbstractContextManager
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.mixins.compute import SemVer
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

class Wheels(AbstractContextManager):
    def __init__(self, ws: WorkspaceClient, install_folder: str, released_version: str):
        self._ws = ws
        self._this_file = Path(__file__)
        self._install_folder = install_folder
        self._released_version = released_version

    def version(self):
        if hasattr(self, "__version"):
            return self.__version
        project_root = self.find_project_root()
        if not (project_root / ".git/config").exists():
            # normal install, downloaded releases won't have the .git folder
            return self._released_version
        try:
            out = subprocess.run(["git", "describe", "--tags"], stdout=subprocess.PIPE, check=True)  # noqa S607
            git_detached_version = out.stdout.decode("utf8")
            dv = SemVer.parse(git_detached_version)
            datestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            # new commits on main branch since the last tag
            new_commits = dv.pre_release.split("-")[0] if dv.pre_release else None
            # show that it's a version different from the released one in stats
            bump_patch = dv.patch + 1
            # create something that is both https://semver.org and https://peps.python.org/pep-0440/
            semver_and_pep0440 = f"{dv.major}.{dv.minor}.{bump_patch}+{new_commits}{datestamp}"
            # validate the semver
            SemVer.parse(semver_and_pep0440)
            self.__version = semver_and_pep0440
            return semver_and_pep0440
        except Exception as err:
            msg = (
                f"Cannot determine unreleased version. Please report this error "
                f"message that you see on https://github.com/databrickslabs/ucx/issues/new. "
                f"Meanwhile, download, unpack, and install the latest released version from "
                f"https://github.com/databrickslabs/ucx/releases. Original error is: {err!s}"
            )
            raise OSError(msg) from None

    def __enter__(self) -> 'Wheels':
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._local_wheel = self._build_wheel(self._tmp_dir.name)
        self._remote_wheel = f"{self._install_folder}/wheels/{self._local_wheel.name}"
        self._remote_dirname = os.path.dirname(self._remote_wheel)
        return self

    def __exit__(self, __exc_type, __exc_value, __traceback):
        self._tmp_dir.cleanup()

    def upload_to_dbfs(self) -> str:
        with self._local_wheel.open("rb") as f:
            self._ws.dbfs.mkdirs(self._remote_dirname)
            logger.info(f"Uploading wheel to dbfs:{self._remote_wheel}")
            self._ws.dbfs.upload(self._remote_wheel, f, overwrite=True)
        return self._remote_wheel

    def upload_to_wsfs(self) -> str:
        with self._local_wheel.open("rb") as f:
            self._ws.workspace.mkdirs(self._remote_dirname)
            logger.info(f"Uploading wheel to /Workspace{self._remote_wheel}")
            self._ws.workspace.upload(self._remote_wheel, f, overwrite=True, format=ImportFormat.AUTO)
        return self._remote_wheel

    def _build_wheel(self, tmp_dir: str, *, verbose: bool = False):
        """Helper to build the wheel package"""
        stdout = subprocess.STDOUT
        stderr = subprocess.STDOUT
        if not verbose:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        project_root = self.find_project_root()
        is_non_released_version = "+" in self.version()
        if (project_root / ".git" / "config").exists() and is_non_released_version:
            tmp_dir_path = Path(tmp_dir) / "working-copy"
            # copy everything to a temporary directory
            shutil.copytree(project_root, tmp_dir_path)
            # and override the version file
            # TODO: make it configurable
            version_file = tmp_dir_path / "src/databricks/labs/ucx/__about__.py"
            with version_file.open("w") as f:
                f.write(f'__version__ = "{self.version()}"')
            # working copy becomes project root for building a wheel
            project_root = tmp_dir_path
        logger.debug(f"Building wheel for {project_root} in {tmp_dir}")
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, project_root.as_posix()],
            check=True,
            stdout=stdout,
            stderr=stderr,
        )
        # get wheel name as first file in the temp directory
        return next(Path(tmp_dir).glob("*.whl"))

    def find_project_root(self) -> Path:
        for leaf in ["pyproject.toml", "setup.py"]:
            root = self._find_dir_with_leaf(self._this_file, leaf)
            if root is not None:
                return root
        msg = "Cannot find project root"
        raise NotADirectoryError(msg)

    @staticmethod
    def _find_dir_with_leaf(folder: Path, leaf: str) -> Path | None:
        root = folder.root
        while str(folder.absolute()) != root:
            if (folder / leaf).exists():
                return folder
            folder = folder.parent
        return None