import abc
import locale
import logging
import os
import pathlib
from functools import cached_property

# pylint: disable-next=import-private-name
from pathlib import Path, _PosixFlavour  # type: ignore
from urllib.parse import quote_from_bytes as urlquote_from_bytes
from io import BytesIO, StringIO

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, DatabricksError
from databricks.sdk.service.workspace import ObjectInfo, ObjectType, ExportFormat, ImportFormat, Language

logger = logging.getLogger(__name__)


class _DatabricksFlavour(_PosixFlavour):
    def __init__(self, ws: WorkspaceClient):
        super().__init__()
        self._ws = ws

    def make_uri(self, path):
        return self._ws.config.host + '#workspace' + urlquote_from_bytes(bytes(path))

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._ws}>"


def _na(fn: str):
    def _inner(*_, **__):
        __tracebackhide__ = True  # pylint: disable=unused-variable
        raise NotImplementedError(f"{fn}() is not available for Databricks Workspace")

    return _inner


class _ScandirItem:
    def __init__(self, object_info):
        self._object_info = object_info

    def __fspath__(self):
        return self._object_info.path

    def is_dir(self):
        return self._object_info.object_type == ObjectType.DIRECTORY

    def is_file(self):
        # TODO: check if we want to show notebooks as files
        return self._object_info.object_type == ObjectType.FILE

    def is_symlink(self):
        return False

    @property
    def name(self):
        return os.path.basename(self._object_info.path)


class _ScandirIterator:
    def __init__(self, objects):
        self._it = objects

    def __iter__(self):
        for object_info in self._it:
            yield _ScandirItem(object_info)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class _DatabricksAccessor:
    chmod = _na('accessor.chmod')
    getcwd = _na('accessor.getcwd')
    group = _na('accessor.group')
    link = _na('accessor.link')
    mkdir = _na('accessor.mkdir')
    owner = _na('accessor.owner')
    readlink = _na('accessor.readlink')
    realpath = _na('accessor.realpath')
    rename = _na('accessor.rename')
    replace = _na('accessor.replace')
    rmdir = _na('accessor.rmdir')
    stat = _na('accessor.stat')
    symlink = _na('accessor.symlink')
    unlink = _na('accessor.unlink')

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def expanduser(self, path):
        home = f"/Users/{self._ws.current_user.me().user_name}"
        return path.replace("~", home)

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._ws}>"

    def scandir(self, path):
        return _ScandirIterator(self._ws.workspace.list(path))

    def listdir(self, path):
        return [item.name for item in self.scandir(path)]


class _UploadIO(abc.ABC):
    def __init__(self, ws: WorkspaceClient, path: str):
        self._ws = ws
        self._path = path

    def close(self):
        # pylint: disable-next=no-member
        io_stream = self.getvalue()  # noqa
        self._ws.workspace.upload(self._path, io_stream, format=ImportFormat.AUTO)

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._path} on {self._ws}>"


class _BinaryUploadIO(_UploadIO, BytesIO):  # type: ignore
    def __init__(self, ws: WorkspaceClient, path: str):
        _UploadIO.__init__(self, ws, path)
        BytesIO.__init__(self)


class _TextUploadIO(_UploadIO, StringIO):  # type: ignore
    def __init__(self, ws: WorkspaceClient, path: str):
        _UploadIO.__init__(self, ws, path)
        StringIO.__init__(self)


class WorkspacePath(Path):
    """Experimental implementation of pathlib.Path for Databricks Workspace."""

    _SUFFIXES = {'.py': Language.PYTHON, '.sql': Language.SQL, '.scala': Language.SCALA, '.R': Language.R}

    _ws: WorkspaceClient
    _flavour: _DatabricksFlavour
    _accessor: _DatabricksAccessor

    cwd = _na('cwd')
    resolve = _na('resolve')
    stat = _na('stat')
    chmod = _na('chmod')
    lchmod = _na('lchmod')
    lstat = _na('lstat')
    owner = _na('owner')
    group = _na('group')
    readlink = _na('readlink')
    symlink_to = _na('symlink_to')
    hardlink_to = _na('hardlink_to')
    touch = _na('touch')
    link_to = _na('link_to')
    samefile = _na('samefile')

    def __new__(cls, ws: WorkspaceClient, path: str | Path):
        this = object.__new__(cls)
        # pathlib does a lot of clever performance tricks, and it's not designed to be subclassed,
        # so we need to set the attributes directly, bypassing the most of a common sense.
        this._flavour = _DatabricksFlavour(ws)
        drv, root, parts = this._parse_args([path])
        return this.__from_raw_parts(this, ws, this._flavour, drv, root, parts)

    @staticmethod
    def __from_raw_parts(this, ws: WorkspaceClient, flavour: _DatabricksFlavour, drv, root, parts) -> 'WorkspacePath':
        # pylint: disable=protected-access
        this._accessor = _DatabricksAccessor(ws)
        this._flavour = flavour
        this._drv = drv
        this._root = root
        this._parts = parts
        this._ws = ws
        return this

    def _make_child_relpath(self, part):
        # used in dir walking
        path = self._flavour.join(self._parts + [part])
        # self._flavour.join duplicates leading '/' (possibly a python bug)
        # but we can't override join in _DatabricksFlavour because it's built-in
        # and if we remove the leading '/' part then we don't get any
        # so let's just do a slow but safe sanity check afterward
        if os.sep == path[0] == path[1]:
            path = path[1:]
        return WorkspacePath(self._ws, path)

    def _parse_args(self, args):  # pylint: disable=arguments-differ
        # instance method adapted from pathlib.Path
        parts = []
        for a in args:
            if isinstance(a, pathlib.PurePath):
                parts += a._parts  # pylint: disable=protected-access
                continue
            parts.append(str(a))
        return self._flavour.parse_parts(parts)

    def _format_parsed_parts(self, drv, root, parts):  # pylint: disable=arguments-differ
        # instance method adapted from pathlib.Path
        if drv or root:
            return drv + root + self._flavour.join(parts[1:])
        return self._flavour.join(parts)

    def _from_parsed_parts(self, drv, root, parts):  # pylint: disable=arguments-differ
        # instance method adapted from pathlib.Path
        this = object.__new__(self.__class__)
        return self.__from_raw_parts(this, self._ws, self._flavour, drv, root, parts)

    def _from_parts(self, args):  # pylint: disable=arguments-differ
        # instance method adapted from pathlib.Path
        drv, root, parts = self._parse_args(args)
        return self._from_parsed_parts(drv, root, parts)

    def relative_to(self, *other) -> pathlib.PurePath:  # type: ignore
        """Databricks Workspace works only with absolute paths, so we make sure to
        return pathlib.Path instead of WorkspacePath to avoid confusion."""
        return pathlib.PurePath(super().relative_to(*other))

    def as_fuse(self):
        """Return FUSE-mounted path in Databricks Runtime."""
        if 'DATABRICKS_RUNTIME_VERSION' not in os.environ:
            logger.warning("This method is only available in Databricks Runtime")
        return Path('/Workspace', self.as_posix())

    def home(self):  # pylint: disable=arguments-differ
        # instance method adapted from pathlib.Path
        return WorkspacePath(self._ws, "~").expanduser()

    def exists(self, *, follow_symlinks=True):
        if not follow_symlinks:
            raise NotImplementedError("follow_symlinks=False is not supported for Databricks Workspace")
        try:
            self._ws.workspace.get_status(self.as_posix())
            return True
        except NotFound:
            return False

    def mkdir(self, mode=0o600, parents=True, exist_ok=True):
        if not exist_ok:
            raise ValueError("exist_ok must be True for Databricks Workspace")
        if not parents:
            raise ValueError("parents must be True for Databricks Workspace")
        if mode != 0o600:
            raise ValueError("other modes than 0o600 are not yet supported")
        self._ws.workspace.mkdirs(self.as_posix())

    def rmdir(self, recursive=False):
        self._ws.workspace.delete(self.as_posix(), recursive=recursive)

    def rename(self, target, overwrite=False):
        dst = WorkspacePath(self._ws, target)
        with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
            self._ws.workspace.upload(dst.as_posix(), f.read(), format=ImportFormat.AUTO, overwrite=overwrite)
        self.unlink()

    def replace(self, target):
        return self.rename(target, overwrite=True)

    def unlink(self, missing_ok=False):
        if not missing_ok and not self.exists():
            raise FileNotFoundError(f"{self.as_posix()} does not exist")
        self._ws.workspace.delete(self.as_posix())

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if encoding is None or encoding == "locale":
            encoding = locale.getpreferredencoding(False)
        if "b" in mode and "r" in mode:
            return self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO)
        if "b" in mode and "w" in mode:
            return _BinaryUploadIO(self._ws, self.as_posix())
        if "r" in mode:
            with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
                return StringIO(f.read().decode(encoding))
        if "w" in mode:
            return _TextUploadIO(self._ws, self.as_posix())
        raise ValueError(f"invalid mode: {mode}")

    @property
    def suffix(self):
        """Return the file extension. If the file is a notebook, return the suffix based on the language."""
        suffix = super().suffix
        if suffix:
            return suffix
        if not self.is_notebook():
            return ""
        for sfx, lang in self._SUFFIXES.items():
            try:
                if self._object_info.language == lang:
                    return sfx
            except DatabricksError:
                return ""
        return ""

    def __lt__(self, other: pathlib.PurePath):
        if not isinstance(other, pathlib.PurePath):
            return NotImplemented
        return self.as_posix() < other.as_posix()

    @cached_property
    def _object_info(self) -> ObjectInfo:
        # this method is cached because it is used in multiple is_* methods.
        # DO NOT use this method in methods, where fresh result is required.
        return self._ws.workspace.get_status(self.as_posix())

    def _return_false(self) -> bool:
        return False

    is_symlink = _return_false
    is_block_device = _return_false
    is_char_device = _return_false
    is_fifo = _return_false
    is_socket = _return_false
    is_mount = _return_false
    is_junction = _return_false

    def is_dir(self):
        try:
            return self._object_info.object_type == ObjectType.DIRECTORY
        except DatabricksError:
            return False

    def is_file(self):
        try:
            return self._object_info.object_type == ObjectType.FILE
        except DatabricksError:
            return False

    def is_notebook(self):
        try:
            return self._object_info.object_type == ObjectType.NOTEBOOK
        except DatabricksError:
            return False

    def __eq__(self, other):
        return isinstance(other, Path) and self.as_posix() == other.as_posix()

    def __hash__(self):
        return Path.__hash__(self)
