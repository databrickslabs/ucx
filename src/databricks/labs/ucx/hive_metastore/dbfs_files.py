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

        # if a test-related jvm_interface is passed in, we don't use py4j's modules
        if jvm_interface:
            self.jvm_filesystem = jvm_interface.jvm.jvm_filesystem
            self.jvm_path = jvm_interface.jvm.jvm_path
        else:
            self.jvm_filesystem = self._jvm.org.apache.hadoop.fs.FileSystem
            self.jvm_path = self._jvm.org.apache.hadoop.fs.Path

    @cached_property
    def _jvm(self):
        try:
            _jvm = self._spark._jvm
            return _jvm
        except Exception as err:
            logger.error(f"Cannot create Py4j proxy: {err}")
            raise err

    @cached_property
    def _fs(self):
        try:
            _jsc = self._spark._jsc  # pylint: disable=protected-access
            return self.jvm_filesystem.get(_jsc.hadoopConfiguration())
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
        path = self.jvm_path(path_str)
        statuses = self._fs.listStatus(path)
        return [self._file_status_to_dbfs_file_info(status) for status in statuses]

    @staticmethod
    def _file_status_to_dbfs_file_info(status):
        return DbfsFileInfo(
            status.getPath().toString(), status.getPath().getName(), status.isDir(), status.getModificationTime()
        )
