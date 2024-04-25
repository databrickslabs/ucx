import abc
import locale
import os
import pathlib
from functools import cached_property

# pylint: disable-next=import-private-name
from pathlib import Path, _PosixFlavour, _Accessor  # type: ignore
from urllib.parse import quote_from_bytes as urlquote_from_bytes
from io import BytesIO, StringIO

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ObjectInfo, ObjectType, ExportFormat, ImportFormat


class _DatabricksFlavour(_PosixFlavour):
    def __init__(self, ws: WorkspaceClient):
        super().__init__()
        self._ws = ws

    def make_uri(self, path):
        return self._ws.config.host + '#workspace' + urlquote_from_bytes(bytes(path))

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._ws}>"


def _na(*args, **kwargs):
    raise NotImplementedError("Not available for Databricks Workspace")


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


class _DatabricksAccessor(_Accessor):
    mkdir = _na
    unlink = _na
    rmdir = _na
    rename = _na
    replace = _na
    stat = _na
    chmod = _na
    link = _na
    symlink = _na
    readlink = _na
    owner = _na
    group = _na
    getcwd = _na
    realpath = _na

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


class WorkspacePath(Path):  # pylint: disable=too-many-public-methods
    """Experimental implementation of pathlib.Path for Databricks Workspace."""

    _ws: WorkspaceClient
    _flavour: _DatabricksFlavour
    _accessor: _DatabricksAccessor

    resolve = _na
    stat = _na
    chmod = _na
    lchmod = _na
    lstat = _na
    owner = _na
    group = _na
    readlink = _na
    symlink_to = _na
    hardlink_to = _na
    touch = _na
    link_to = _na
    samefile = _na

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

    @classmethod
    def cwd(cls):
        # should we?..
        raise NotImplementedError("Not available for Databricks Workspace")

    def absolute(self):
        # TODO: check if this is correct
        return super().absolute()

    @cached_property
    def _object_info(self) -> ObjectInfo:
        # this method is cached because it is used in multiple is_* methods.
        # DO NOT use this method in methods, where fresh result is required.
        return self._ws.workspace.get_status(self.as_posix())

    @staticmethod
    def _return_false():
        return False

    is_symlink = _return_false
    is_block_device = _return_false
    is_char_device = _return_false
    is_fifo = _return_false
    is_socket = _return_false
    is_mount = _return_false
    is_junction = _return_false

    def is_dir(self):
        return self._object_info.object_type == ObjectType.DIRECTORY

    def is_file(self):
        return self._object_info.object_type == ObjectType.FILE

    def is_notebook(self):
        return self._object_info.object_type == ObjectType.NOTEBOOK
