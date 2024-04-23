import abc
import os
import pathlib
from functools import cached_property
from pathlib import Path, _PosixFlavour, _Accessor  # type: ignore
from io import BytesIO, StringIO
from databricks.sdk import WorkspaceClient
from urllib.parse import quote_from_bytes as urlquote_from_bytes

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



class _DatabricksAccessor(_Accessor):
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def _na(self):
        raise NotImplementedError("Not available for Databricks Workspace")

    stat = _na
    chmod = _na
    link = _na
    symlink = _na
    readlink = _na
    #
    # def open(self):
    #     return os.open()

    def expanduser(self, path):
        home = f"/Users/{self._ws.current_user.me().user_name}"
        return path.replace("~", home)

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._ws}>"

    #
    # open = io.open
    # listdir = os.listdir
    # scandir = os.scandir
    # mkdir = os.mkdir
    # unlink = os.unlink
    # rmdir = os.rmdir
    # rename = os.rename
    # replace = os.replace
    #
    # def touch(self, path, mode=0o666, exist_ok=True):
    #     if exist_ok:
    #         # First try to bump modification time
    #         # Implementation note: GNU touch uses the UTIME_NOW option of
    #         # the utimensat() / futimens() functions.
    #         try:
    #             os.utime(path, None)
    #         except OSError:
    #             # Avoid exception chaining
    #             pass
    #         else:
    #             return
    #     flags = os.O_CREAT | os.O_WRONLY
    #     if not exist_ok:
    #         flags |= os.O_EXCL
    #     fd = os.open(path, flags, mode)
    #     os.close(fd)
    #
    owner = _na
    group = _na
    getcwd = _na
    # getcwd = os.getcwd
    # realpath = staticmethod(os.path.realpath)


class _UploadIO(abc.ABC):
    def __init__(self, ws: WorkspaceClient, path: str):
        self._ws = ws
        self._path = path

    # @abc.abstractmethod
    # def getvalue(self):
    #     pass

    def close(self):
        x = self.getvalue()
        self._ws.workspace.upload(self._path, x, format=ImportFormat.AUTO)

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._path} on {self._ws}>"


class _BinaryUploadIO(_UploadIO, BytesIO):
    def __init__(self, ws: WorkspaceClient, path: str):
        _UploadIO.__init__(self, ws, path)
        BytesIO.__init__(self)


class _TextUploadIO(_UploadIO, StringIO):
    def __init__(self, ws: WorkspaceClient, path: str):
        _UploadIO.__init__(self, ws, path)
        StringIO.__init__(self)


class WorkspacePath(Path):
    def __new__(cls, ws: WorkspaceClient, path: str | Path):
        self = object.__new__(cls)
        self._flavour = _DatabricksFlavour(ws)
        drv, root, parts = self._parse_args([path])
        return self.__from_raw_parts(self, ws, self._flavour, drv, root, parts)

    @staticmethod
    def __from_raw_parts(self, ws: WorkspaceClient, flavour: _DatabricksFlavour, drv, root, parts) -> 'WorkspacePath':
        self._accessor = _DatabricksAccessor(ws)
        self._flavour = flavour
        self._drv = drv
        self._root = root
        self._parts = parts
        self._ws = ws
        return self

    def _parse_args(self, args):
        """This instance method is adapted from a @classmethod of pathlib.Path"""
        parts = []
        for a in args:
            if isinstance(a, pathlib.PurePath):
                parts += a._parts
                continue
            parts.append(str(a))
        return self._flavour.parse_parts(parts)

    def _make_child_relpath(self, part):
        # used in dir walking
        path = self._flavour.join(self._parts + [part])
        return WorkspacePath(self._ws, path)

    def _format_parsed_parts(self, drv, root, parts):
        # instance method adapted from pathlib.Path
        if drv or root:
            return drv + root + self._flavour.join(parts[1:])
        return self._flavour.join(parts)

    def _from_parsed_parts(self, drv, root, parts):
        # instance method adapted from pathlib.Path
        this = object.__new__(self.__class__)
        return self.__from_raw_parts(this, self._ws, self._flavour, drv, root, parts)

    def _from_parts(self, args):
        # instance method adapted from pathlib.Path
        drv, root, parts = self._parse_args(args)
        return self._from_parsed_parts(drv, root, parts)

    def exists(self, *, follow_symlinks=True):
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

    def rename(self, target):
        dst = WorkspacePath(self._ws, target)
        with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
            self._ws.workspace.upload(dst.as_posix(), f.read(), format=ImportFormat.AUTO)
        self.unlink()

    def unlink(self, missing_ok=False):
        if not missing_ok and not self.exists():
            raise FileNotFoundError(f"{self.as_posix()} does not exist")
        self._ws.workspace.delete(self.as_posix())

    def home(self):
        return WorkspacePath(self._ws, "~").expanduser()

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if "b" in mode and "r" in mode:
            return self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO)
        if "b" in mode and "w" in mode:
            return _BinaryUploadIO(self._ws, self.as_posix())
        if "r" in mode:
            with self._ws.workspace.download(self.as_posix(), format=ExportFormat.AUTO) as f:
                return StringIO(f.read().decode(encoding or "utf-8"))
        if "w" in mode:
            return _TextUploadIO(self._ws, self.as_posix())
        raise ValueError(f"invalid mode: {mode}")

    @cached_property
    def _object_info(self) -> ObjectInfo:
        # this method is cached because it is used in multiple is_* methods.
        # DO NOT use this method in methods, where fresh result is required.
        return self._ws.workspace.get_status(self.as_posix())

    def is_dir(self):
        return self._object_info.object_type == ObjectType.DIRECTORY

    def is_file(self):
        return self._object_info.object_type == ObjectType.FILE

    def is_notebook(self):
        return self._object_info.object_type == ObjectType.NOTEBOOK

    def is_symlink(self):
        return False

    def is_socket(self):
        return False

    def is_fifo(self):
        return False

    def is_block_device(self):
        return False

    def is_char_device(self):
        return False

    def is_junction(self):
        return False

    ######################################################################
    # The following methods are not implemented for Databricks Workspace #
    ######################################################################

    @classmethod
    def cwd(cls):
        raise NotImplementedError("Not available for Databricks Workspace")

    def stat(self, *, follow_symlinks=True):
        raise NotImplementedError("Not available for Databricks Workspace")

    def chmod(self, mode, *, follow_symlinks=True):
        raise NotImplementedError("Not available for Databricks Workspace")

    def lchmod(self, mode):
        raise NotImplementedError("Not available for Databricks Workspace")

    def lstat(self):
        raise NotImplementedError("Not available for Databricks Workspace")

    def owner(self):
        raise NotImplementedError("Not available for Databricks Workspace")

    def group(self):
        raise NotImplementedError("Not available for Databricks Workspace")

    def is_mount(self):
        return False

    def readlink(self):
        raise NotImplementedError("Not available for Databricks Workspace")

    def replace(self, target):
        return super().replace(target)

    def resolve(self, strict=False):
        return super().resolve(strict)

    def symlink_to(self, target, target_is_directory=False):
        raise NotImplementedError("Not available for Databricks Workspace")

    def hardlink_to(self, target):
        raise NotImplementedError("Not available for Databricks Workspace")

    def touch(self, mode=0o666, exist_ok=True):
        super().touch(mode, exist_ok)

    def absolute(self):
        return super().absolute()

    def read_bytes(self):
        return super().read_bytes()

    def read_text(self, encoding=None, errors=None):
        return super().read_text(encoding, errors)

    def samefile(self, other_path):
        return super().samefile(other_path)

    def link_to(self, target):
        raise NotImplementedError("Not available for Databricks Workspace")

    def walk(self, top_down=..., on_error=..., follow_symlinks=...):
        return super().walk(top_down, on_error, follow_symlinks)
