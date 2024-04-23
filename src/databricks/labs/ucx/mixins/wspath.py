import os
import pathlib
from functools import cached_property
from pathlib import Path, _PosixFlavour, _Accessor  # type: ignore

from databricks.sdk import WorkspaceClient
from urllib.parse import quote_from_bytes as urlquote_from_bytes
from databricks.sdk.service.workspace import ObjectInfo, ObjectType


class _DatabricksFlavour(_PosixFlavour):
    def __init__(self, ws: WorkspaceClient):
        super().__init__()
        self._ws = ws

    def make_uri(self, path):
        return self._ws.config.host + '#workspace' + urlquote_from_bytes(bytes(path))



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

    def open(self):
        return os.open()

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
    # expanduser = staticmethod(os.path.expanduser)
    # realpath = staticmethod(os.path.realpath)

class WorkspacePath(Path):
    def __new__(cls, ws: WorkspaceClient, path: str | Path, *, object_info: ObjectInfo | None = None):
        self = object.__new__(cls)
        self._flavour = _DatabricksFlavour(ws)
        drv, root, parts = self._parse_args([path])
        self._drv = drv
        self._root = root
        self._parts = parts
        self._ws = ws
        if object_info is not None:
            # If object_info is not none, we can avoid an extra API call
            self.__dict__["_object_info"] = object_info
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

    def iterdir(self):
        for object_info in self._ws.workspace.list(self.as_posix()):
            yield WorkspacePath(self._ws, object_info.path, object_info=object_info)

    def _make_child_relpath(self, part):
        # This is an optimization used for dir walking.  `part` must be
        # a single part relative to this path.
        parts = self._parts + [part]
        return self._from_parsed_parts(self._drv, self._root, parts)

    def _format_parsed_parts(self, drv, root, parts):
        # instance method adapted from pathlib.Path
        if drv or root:
            return drv + root + self._flavour.join(parts[1:])
        return self._flavour.join(parts)

    @classmethod
    def _from_parsed_parts(cls, drv, root, parts):
        self = object.__new__(cls)
        self._drv = drv
        self._root = root
        self._parts = parts
        return self

    def exists(self, *, follow_symlinks=True):
        try:
            _ = self._object_info
            return True
        except FileNotFoundError:
            return False

    @cached_property
    def _object_info(self) -> ObjectInfo:
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

    def glob(self, pattern, *, case_sensitive=None):
        return super().glob(pattern, case_sensitive=case_sensitive)

    def rglob(self, pattern, *, case_sensitive=None):
        return super().rglob(pattern, case_sensitive=case_sensitive)

    def lchmod(self, mode):
        super().lchmod(mode)

    def lstat(self):
        return super().lstat()

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        super().mkdir(mode, parents, exist_ok)

    # @overload
    # def open(
    #     self,
    #     mode: OpenTextMode = "r",
    #     buffering: int = -1,
    #     encoding: str | None = None,
    #     errors: str | None = None,
    #     newline: str | None = None,
    # ) -> TextIOWrapper: ...
    #
    # @overload
    # def open(
    #     self, mode: OpenBinaryMode, buffering: Literal[0], encoding: None = None, errors: None = None, newline: None = None
    # ) -> FileIO: ...
    #
    # @overload
    # def open(
    #     self,
    #     mode: OpenBinaryModeUpdating,
    #     buffering: Literal[-1, 1] = -1,
    #     encoding: None = None,
    #     errors: None = None,
    #     newline: None = None,
    # ) -> BufferedRandom: ...
    #
    # @overload
    # def open(
    #     self,
    #     mode: OpenBinaryModeWriting,
    #     buffering: Literal[-1, 1] = -1,
    #     encoding: None = None,
    #     errors: None = None,
    #     newline: None = None,
    # ) -> BufferedWriter: ...
    #
    # @overload
    # def open(
    #     self,
    #     mode: OpenBinaryModeReading,
    #     buffering: Literal[-1, 1] = -1,
    #     encoding: None = None,
    #     errors: None = None,
    #     newline: None = None,
    # ) -> BufferedReader: ...
    #
    # @overload
    # def open(
    #     self, mode: pathlib.OpenBinaryMode, buffering: int = -1, encoding: None = None, errors: None = None, newline: None = None
    # ) -> BinaryIO: ...
    #
    # @overload
    # def open(
    #     self, mode: str, buffering: int = -1, encoding: str | None = None, errors: str | None = None, newline: str | None = None
    # ) -> IO[Any]: ...

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        return super().open(mode, buffering, encoding, errors, newline)

    def owner(self):
        return super().owner()

    def group(self):
        return super().group()

    def is_mount(self):
        return super().is_mount()

    def readlink(self):
        return super().readlink()

    def rename(self, target):
        return super().rename(target)

    def replace(self, target):
        return super().replace(target)

    def resolve(self, strict=False):
        return super().resolve(strict)

    def rmdir(self):
        super().rmdir()

    def symlink_to(self, target, target_is_directory=False):
        super().symlink_to(target, target_is_directory)

    def hardlink_to(self, target):
        super().hardlink_to(target)

    def touch(self, mode=0o666, exist_ok=True):
        super().touch(mode, exist_ok)

    def unlink(self, missing_ok=False):
        super().unlink(missing_ok)

    @classmethod
    def home(cls):
        return super().home()

    def absolute(self):
        return super().absolute()

    def expanduser(self):
        return super().expanduser()

    def read_bytes(self):
        return super().read_bytes()

    def read_text(self, encoding=None, errors=None):
        return super().read_text(encoding, errors)

    def samefile(self, other_path):
        return super().samefile(other_path)

    def write_bytes(self, data):
        return super().write_bytes(data)

    def write_text(self, data, encoding=None, errors=None, newline=None):
        return super().write_text(data, encoding, errors, newline)

    def link_to(self, target):
        super().link_to(target)

    def walk(self, top_down=..., on_error=..., follow_symlinks=...):
        return super().walk(top_down, on_error, follow_symlinks)