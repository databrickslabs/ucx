from __future__ import annotations

import fnmatch
import logging
import os
import posixpath
import re

from pathlib import Path, PurePath
from typing import NoReturn

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DbfsPath(Path):
    """Experimental implementation of pathlib.Path for DBFS URIs."""

    # Implementation notes:
    #  - The builtin Path classes are not designed for extension, which in turn makes everything a little cumbersome.
    #  - The internals of the builtin pathlib have changed dramatatically across supported Python versions (3.10-3.12
    #    at the time of writing) so relying on those details is brittle. (Python 3.13 also includes a significant
    #    refactoring.)
    #  - Until 3.11 the implementation was decomposed and delegated to two internal interfaces:
    #     1. Flavour (scope=class) which encapsulates the path style and manipulation.
    #     2. Accessor (scope=instance) to which the I/O-related calls are delegated.
    #    These interfaces are internal/protected.
    #  - Since 3.12 the implementation of these interfaces have been removed:
    #     1. Flavour has been replaced with os.path (posixpath or ntpath). (Still class-scoped.)
    #     2. Accessor has been replaced with inline implementations based directly on the 'os' module.
    #
    # This implementation for DBFS does the following:
    #     1. Flavour is posix-style separators, with the dbfs: prefix handled as a drive. Paths must be absolute.
    #     2. The Accessor is delegated to existing routines available via the workspace client.
    #
    # Although there is a lot of duplication and similarity with the WorkspacePath implementation, the initial focus
    # is on implementation with factoring out common functionality deferred for now.
    #
    __slots__ = (
        # For us this is always the empty string. Consistent with the superclass attribute for Python 3.10-3.13b.
        '_drv',
        # The (normalized) root property for the path. Consistent with the superclass attribute for Python 3.10-3.13b.
        '_root',
        # The (normalized) path components (relative to the root) for the path.
        # For python <=3.11 this supersedes _parts
        # For python 3.12+ this supersedes _raw_paths
        '_path_parts',
        # The cached str() representation of the instance. Consistent with the superclass attribute for Python 3.10-3.13b.
        '_str',
        # The cached hash() value for the instance. Consistent with the superclass attribute for Python 3.10-3.13b.
        '_hash',
        # The workspace client that we use to perform I/O operations on the path.
        '_ws',
    )

    # Path semantics are posix-like.
    parser = posixpath

    # Compatibility attribute; for when superclass implementations get invoked on python thru 3.11
    _flavour = object()

    @classmethod
    def __new__(cls, *args, **kwargs) -> DbfsPath:
        # Force all initialisation to go via __init__() irrespective of the (Python-specific) base version.
        return object.__new__(cls)

    def __init__(self, *args, ws: WorkspaceClient) -> None:
        raw_paths: list[str] = []
        for arg in args:
            if isinstance(arg, PurePath):
                raw_paths.extend(arg.parts)
            else:
                try:
                    path = os.fspath(arg)
                except TypeError:
                    path = arg
                if not isinstance(path, str):
                    msg = (
                        f"argument should be a str or an os.PathLib object where __fspath__ returns a str, "
                        f"not {type(path).__name__!r}"
                    )
                    raise TypeError(msg)
                raw_paths.append(path)

        # Normalise the paths that we have.
        root, path_parts = self._parse_and_normalize(raw_paths)

        self._drv = ""
        self._root = root
        self._path_parts = path_parts
        self._ws = ws

    @classmethod
    def _parse_and_normalize(cls, parts: list[str]) -> tuple[str, tuple[str, ...]]:
        """Parse and normalize a list of path components.

        Args:
            parts: a list of path components to parse and normalize.
        Returns:
            A tuple containing:
              - The normalized drive (always '')
              - The normalized root for this path, or '' if there isn't any.
              - The normalized path components, if any, (relative) to the root.
        """
        match parts:
            case []:
                path = ''
            case [part]:
                path = part
            case [*parts]:
                path = cls.parser.join(*parts)
        if path:
            root, rel = cls._splitroot(path, sep=cls.parser.sep)
            # No need to split drv because we don't support it.
            parsed = tuple(str(x) for x in rel.split(cls.parser.sep) if x and x != '.')
        else:
            root, parsed = '', ()
        return root, parsed

    @classmethod
    def _splitroot(cls, part: str, sep: str) -> tuple[str, str]:
        # Based on the upstream implementation, with the '//'-specific bit elided because we don't need to
        # bother with Posix semantics.
        if part and part[0] == sep:
            root, path = sep, part.lstrip(sep)
        else:
            root, path = '', part
        return root, path

    def __reduce__(self) -> NoReturn:
        # Cannot support pickling because we can't pickle the workspace client.
        msg = "Pickling DBFS paths is not supported."
        raise NotImplementedError(msg)

    def __fspath__(self):
        # Cannot support this: DBFS objects aren't accessible via the filesystem.
        msg = f"DBFS paths are not path-like: {self}"
        raise NotImplementedError(msg)

    def as_posix(self):
        return str(self)

    def __str__(self):
        try:
            return self._str
        except AttributeError:
            self._str = (self._root + self.parser.sep.join(self._path_parts)) or '.'
            return self._str

    def as_uri(self):
        if not self.is_absolute():
            raise ValueError("Relative path can't be expressed as a DBFS URI")
        return f"dbfs:{self}"

    def __bytes__(self):
        # Super implementations are fine.
        return super(self).__bytes__()

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)!r})"

    def __eq__(self, other):
        if not isinstance(other, DbfsPath):
            return NotImplemented
        return str(self) == str(other)

    def __hash__(self):
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(str(self))
            return self._hash

    def _parts(self) -> tuple[str, ...]:
        """Return a tuple that has the same natural ordering as paths of this type."""
        return (self._root, *self._path_parts)

    @property
    def _cparts(self):
        # Compatibility property (python <= 3.11), accessed via reverse equality comparison. This can't be avoided.
        return self._parts()

    def __lt__(self, other):
        if not isinstance(other, DbfsPath):
            return NotImplemented
        return self._path_parts < other._path_parts

    def __le__(self, other):
        if not isinstance(other, DbfsPath):
            return NotImplemented
        return self._path_parts <= other._path_parts

    def __gt__(self, other):
        if not isinstance(other, DbfsPath):
            return NotImplemented
        return self._path_parts > other._path_parts

    def __ge__(self, other):
        if not isinstance(other, DbfsPath):
            return NotImplemented
        return self._path_parts >= other._path_parts

    def with_segments(self, *pathsegments):
        return type(self)(*pathsegments, ws=self._ws)

    @property
    def drive(self) -> str:
        return self._drv

    @property
    def root(self):
        return self._root

    @property
    def anchor(self):
        return self.drive + self.root

    @property
    def name(self):
        path_parts = self._path_parts
        return path_parts[-1] if path_parts else ''

    @property
    def parts(self):
        if self.drive or self.root:
            parts = (self.drive + self.root, *self._path_parts)
        else:
            parts = self._path_parts
        return parts

    @property
    def suffix(self):
        # Super implementations are all fine.
        return super().suffix

    @property
    def suffixes(self):
        # Super implementations are all fine.
        return super().suffixes

    @property
    def stem(self):
        # Super implementations are all fine.
        return super().stem

    def with_name(self, name):
        parser = self.parser
        if not name or parser.sep in name or name == '.':
            msg = f"Invalid name: {name!r}"
            raise ValueError(msg)
        path_parts = list(self._path_parts)
        if not path_parts:
            raise ValueError(f"{self!r} has an empty name")
        path_parts[-1] = name
        return type(self)(self.anchor, *path_parts, ws=self._ws)

    def with_stem(self, stem):
        # Super implementations are all fine.
        return super().with_stem(stem)

    def with_suffix(self, suffix):
        stem = self.stem
        if not stem:
            msg = f"{self!r} has an empty name"
            raise ValueError(msg)
        elif suffix and not suffix.startswith('.'):
            msg = f"{self!r} invalid suffix: {suffix}"
            raise ValueError(msg)
        else:
            return self.with_name(stem + suffix)

    @property
    def _stack(self):
        return self.anchor, list(reversed(self._path_parts))

    def relative_to(self, other, *more_other, walk_up=False):
        other = self.with_segments(other, *more_other)
        anchor0, parts0 = self._stack
        anchor1, parts1 = other._stack
        if anchor0 != anchor1:
            msg = f"{str(self)!r} and {str(other)!r} have different anchors"
            raise ValueError(msg)
        while parts0 and parts1 and parts0[-1] == parts1[-1]:
            parts0.pop()
            parts1.pop()
        for part in parts1:
            if not walk_up:
                msg = f"{str(self)!r} is not in the subpath of {str(other)!r}"
                raise ValueError(msg)
            elif part == '..':
                raise ValueError(f"'..' segment in {str(other)!r} cannot be walked")
            else:
                parts0.append('..')
        return self.with_segments('', *reversed(parts0))

    def is_relative_to(self, other, *more_other):
        other = self.with_segments(other, *more_other)
        if self.anchor != other.anchor:
            return False
        parts0 = list(reversed(self._path_parts))
        parts1 = list(reversed(other._path_parts))
        while parts0 and parts1 and parts0[-1] == parts1[-1]:
            parts0.pop()
            parts1.pop()
        for part in parts1:
            if part and part != '.':
                return False
        return True

    @property
    def parent(self):
        rel_path = self._path_parts
        return self.with_segments(self.anchor, *rel_path[:-1]) if rel_path else self

    @property
    def parents(self):
        parents = []
        path = self
        parent = path.parent
        while path != parent:
            parents.append(parent)
            path = parent
            parent = path.parent
        return tuple(parents)

    def is_absolute(self):
        return bool(self.anchor)

    def is_reserved(self):
        return False

    def joinpath(self, *pathsegments):
        return self.with_segments(self, *pathsegments)

    @classmethod
    def _compile_pattern(cls, pattern: str, case_sensitive: bool) -> re.Pattern:
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = fnmatch.translate(pattern)
        return re.compile(regex, flags=flags)

    def match(self, path_pattern, *, case_sensitive=None):
        # Convert the pattern to a fake path (with globs) to help with matching parts.
        if not isinstance(path_pattern, PurePath):
            path_pattern = self.with_segments(path_pattern)
        # Default to false if not specified.
        if case_sensitive is None:
            case_sensitive = True
        # Reverse the parts.
        path_parts = self.parts
        pattern_parts = path_pattern.parts
        # Error
        if not pattern_parts:
            raise ValueError("empty pattern")
        # Impossible matches.
        if len(path_parts) < len(pattern_parts) or len(path_parts) > len(pattern_parts) and path_pattern.anchor:
            return False
        # Check each part.
        for path_part, pattern_part in zip(reversed(path_parts), reversed(pattern_parts)):
            pattern = self._compile_pattern(pattern_part, case_sensitive=case_sensitive)
            if not pattern.match(path_part):
                return False
        return True
