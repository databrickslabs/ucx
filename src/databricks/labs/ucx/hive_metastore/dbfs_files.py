import logging
from dataclasses import dataclass
from functools import cached_property

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DbfsFileInfo:
    path: str
    name: str
    is_dir: bool
    modification_time: int


class DbfsFiles:

    def __init__(self, jvm_interface=None):
        # pylint: disable=import-error,import-outside-toplevel
        if jvm_interface:
            self._spark = jvm_interface
        else:
            try:
                from pyspark.sql.session import SparkSession  # type: ignore[import-not-found]

                self._spark = SparkSession.builder.getOrCreate()
            except Exception as err:
                logger.error(f"Unable to get SparkSession: {err}")
                raise err

        # if a test-related jvm_interface is passed in, we don't use py4j's java_import
        self._java_import = self._noop if jvm_interface else self._default_java_import

    @staticmethod
    def _noop(*args, **kwargs):
        pass

    @staticmethod
    def _default_java_import(jvm, import_path: str):
        # pylint: disable=import-outside-toplevel
        from py4j.java_gateway import java_import  # type: ignore[import]

        java_import(jvm, import_path)

    @cached_property
    def _jvm(self):
        try:
            _jvm = self._spark._jvm
            self._java_import(_jvm, "org.apache.hadoop.fs.FileSystem")
            self._java_import(_jvm, "org.apache.hadoop.fs.Path")
            return _jvm
        except Exception as err:
            logger.error(f"Cannot create Py4j proxy: {err}")
            raise err

    @cached_property
    def _fs(self):
        try:
            _jsc = self._spark._jsc  # pylint: disable=protected-access
            return self._jvm.FileSystem.get(_jsc.hadoopConfiguration())
        except Exception as err:
            logger.error(f"Cannot create Py4j file system proxy: {err}")
            raise err

    class InvalidPathFormatError(ValueError):
        pass

    def validate_path(self, path: str) -> None:
        if not path.startswith("dbfs:/"):
            raise self.InvalidPathFormatError(f"Input path should begin with 'dbfs:/' prefix. Input path: '{path}'")

    def list_dir(self, path: str) -> list[DbfsFileInfo]:
        self.validate_path(path)
        return self._list_dir(path)

    def _list_dir(self, path_str: str) -> list[DbfsFileInfo]:
        path = self._jvm.Path(path_str)
        statuses = self._fs.listStatus(path)
        return [self._file_status_to_dbfs_file_info(status) for status in statuses]

    @staticmethod
    def _file_status_to_dbfs_file_info(status):
        return DbfsFileInfo(
            status.getPath().toString(), status.getPath().getName(), status.isDir(), status.getModificationTime()
        )
