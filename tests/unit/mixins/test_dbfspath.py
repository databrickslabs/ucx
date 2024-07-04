import os
from pathlib import Path, PurePosixPath, PureWindowsPath, PurePath

import pytest

from databricks.labs.ucx.mixins.dbfspath import DbfsPath

from .. import mock_workspace_client


def test_empty_init() -> None:
    """Ensure that basic initialization works."""
    ws = mock_workspace_client()

    # Simple initialisation.
    DbfsPath(ws=ws)


@pytest.mark.parametrize(
    ("args", "expected"), [
        # Some absolute paths.
        (["/foo/bar"], ("/", ("/", "foo", "bar"))),
        (["/", "foo", "bar"], ("/", ("/", "foo", "bar"))),
        (["/", "foo/bar"], ("/", ("/", "foo", "bar"))),
        (["/", "foo", "bar/baz"], ("/", ("/", "foo", "bar", "baz"))),
        # Some relative paths.
        (["foo/bar"], ("", ("foo", "bar"))),
        (["foo", "bar"], ("", ("foo", "bar"))),
        (["foo", "/bar"], ("/", ("/", "bar"))),
        # Some paths with mixed components of Path types.
        (["/", "foo", PurePosixPath("bar/baz")], ("/", ("/", "foo", "bar", "baz"))),
        (["/", "foo", PureWindowsPath("config.sys")], ("/", ("/", "foo", "config.sys"))),
        (["/", "foo", PurePath("bar")], ("/", ("/", "foo", "bar"))),
        # Some corner cases: empty, root and trailing '/'.
        ([], ("", ())),
        (["/"], ("/", ("/",))),
        (["/foo/"], ("/", ("/", "foo"))),
        # Intermediate '.' are supposed to be dropped during normalization.
        (["/foo/./bar"], ("/", ("/", "foo", "bar"))),
    ],
    ids=lambda param: f"DbfsPath({param!r})" if isinstance(param, list) else repr(param)
)
def test_init(args: list[str | PurePath], expected: tuple[str, list[str]]) -> None:
    """Ensure that initialization with various combinations of segments works as expected."""
    ws = mock_workspace_client()

    # Run the test.
    p = DbfsPath(*args, ws=ws)

    # Validate the initialisation results.
    assert (p.drive, p.root, p.parts) == ('', *expected)


def test_init_error() -> None:
    """Ensure that we detect initialisation with non-string or path-like path components."""
    ws = mock_workspace_client()

    expected_msg = f"argument should be a str or an os.PathLib object where __fspath__ returns a str, not 'int'"
    with pytest.raises(TypeError, match=expected_msg):
        DbfsPath("foo", "bar", 12, ws=ws)


def test_equality() -> None:
    """Test that DBFS paths can be compared with each other for equality."""
    ws = mock_workspace_client()

    assert DbfsPath("/foo/bar", ws=ws) == DbfsPath("/", "foo", "bar", ws=ws)
    assert DbfsPath("foo/bar", ws=ws) == DbfsPath("foo", "bar", ws=ws)
    assert DbfsPath("/foo/bar", ws=ws) != DbfsPath("foo", "bar", ws=ws)

    assert DbfsPath("/foo/bar", ws=ws) != Path("foo", "bar", ws=ws)
    assert Path("/foo/bar") != DbfsPath("/foo/bar", ws=ws)


def test_hash() -> None:
    """Test that equal DBFS paths have the same hash value."""
    ws = mock_workspace_client()

    assert hash(DbfsPath("/foo/bar", ws=ws)) == hash(DbfsPath("/", "foo", "bar", ws=ws))
    assert hash(DbfsPath("foo/bar", ws=ws)) == hash(DbfsPath("foo", "bar", ws=ws))


@pytest.mark.parametrize(
    "increasing_paths", [
        ("/foo", "/foo/bar", "/foo/baz"),
        ("foo", "foo/bar", "foo/baz"),
    ]
)
def test_comparison(increasing_paths: tuple[str | list[str], str | list[str], str | list[str]]) -> None:
    """Test that comparing paths works as expected."""
    ws = mock_workspace_client()

    p1, p2, p3 = (DbfsPath(p, ws=ws) for p in increasing_paths)
    assert p1 < p2 < p3
    assert p1 <= p2 <= p2 <= p3
    assert p3 > p2 > p1
    assert p3 >= p2 >= p2 > p1


def test_comparison_errors() -> None:
    """Test that comparing DBFS paths with other types yields the error we expect, irrespective of comparison order."""
    ws = mock_workspace_client()

    with pytest.raises(TypeError, match="'<' not supported between instances"):
        _ = DbfsPath("foo", ws=ws) < PurePosixPath("foo")
    with pytest.raises(TypeError, match="'>' not supported between instances"):
        _ = DbfsPath("foo", ws=ws) > PurePosixPath("foo")
    with pytest.raises(TypeError, match="'<=' not supported between instances"):
        _ = DbfsPath("foo", ws=ws) <= PurePosixPath("foo")
    with pytest.raises(TypeError, match="'>=' not supported between instances"):
        _ = DbfsPath("foo", ws=ws) >= PurePosixPath("foo")
    with pytest.raises(TypeError, match="'<' not supported between instances"):
        _ = PurePosixPath("foo") < DbfsPath("foo", ws=ws)
    with pytest.raises(TypeError, match="'>' not supported between instances"):
        _ = PurePosixPath("foo") > DbfsPath("foo", ws=ws)
    with pytest.raises(TypeError, match="'<=' not supported between instances"):
        _ = PurePosixPath("foo") <= DbfsPath("foo", ws=ws)
    with pytest.raises(TypeError, match="'>=' not supported between instances"):
        _ = PurePosixPath("foo") >= DbfsPath("foo", ws=ws)


def test_drive() -> None:
    """Test that the drive is empty for our paths."""
    ws = mock_workspace_client()

    assert DbfsPath("/foo", ws=ws).drive == ''
    assert DbfsPath("foo", ws=ws).root == ''


def test_root() -> None:
    """Test that absolute paths have the '/' root and relative paths do not."""
    # More comprehensive tests are part of test_init()
    ws = mock_workspace_client()

    assert DbfsPath("/foo", ws=ws).root == '/'
    assert DbfsPath("foo", ws=ws).root == ''


def test_anchor() -> None:
    """Test that the anchor for absolute paths is '/' and empty for relative paths."""
    ws = mock_workspace_client()

    assert DbfsPath("/foo", ws=ws).anchor == '/'
    assert DbfsPath("foo", ws=ws).anchor == ''


def test_pathlike_error() -> None:
    """Paths are "path-like" but DBFS ones aren't, so verify that triggers an error."""
    ws = mock_workspace_client()
    p = DbfsPath("/some/path", ws=ws)

    with pytest.raises(NotImplementedError, match="DBFS paths are not path-like"):
        _ = os.fspath(p)


def test_name() -> None:
    """Test that the last part of the path is properly noted as the name."""
    ws = mock_workspace_client()

    assert DbfsPath("/foo/bar", ws=ws).name == "bar"
    assert DbfsPath("/foo/", ws=ws).name == "foo"
    assert DbfsPath("/", ws=ws).name == ""
    assert DbfsPath(ws=ws).name == ""


def test_parts() -> None:
    """Test that parts returns the anchor and path components."""
    # More comprehensive tests are part of test_init()
    ws = mock_workspace_client()

    assert DbfsPath("/foo/bar", ws=ws).parts == ("/", "foo", "bar")
    assert DbfsPath("/foo/", ws=ws).parts == ("/", "foo")
    assert DbfsPath("/", ws=ws).parts == ("/",)
    assert DbfsPath("foo/bar", ws=ws).parts == ("foo", "bar")
    assert DbfsPath("foo/", ws=ws).parts == ("foo",)


def test_suffix() -> None:
    """Test that the suffix is correctly extracted."""
    ws = mock_workspace_client()

    assert DbfsPath("/path/to/distribution.tar.gz", ws=ws).suffix == ".gz"
    assert DbfsPath("/no/suffix/here", ws=ws).suffix == ""


def test_suffixes() -> None:
    """Test that multiple suffixes are correctly extracted."""
    ws = mock_workspace_client()

    assert DbfsPath("/path/to/distribution.tar.gz", ws=ws).suffixes == [".tar", ".gz"]
    assert DbfsPath("/path/to/file.txt", ws=ws).suffixes == [".txt"]
    assert DbfsPath("/no/suffix/here", ws=ws).suffixes == []


def test_stem() -> None:
    """Test that the stem is correctly extracted."""
    ws = mock_workspace_client()

    assert DbfsPath("/path/to/distribution.tar.gz", ws=ws).stem == "distribution.tar"
    assert DbfsPath("/path/to/file.txt", ws=ws).stem == "file"
    assert DbfsPath("/no/suffix/here", ws=ws).stem == "here"


def test_with_name() -> None:
    """Test that the name in a path can be replaced."""
    ws = mock_workspace_client()

    assert DbfsPath("/path/to/notebook.py", ws=ws).with_name("requirements.txt") == DbfsPath("/path/to/requirements.txt", ws=ws)
    assert DbfsPath("relative/notebook.py", ws=ws).with_name("requirements.txt") == DbfsPath("relative/requirements.txt", ws=ws)


@pytest.mark.parametrize(
    ("path", "name"), [
        # Invalid names.
        ("/a/path", "invalid/replacement"),
        ("/a/path", ""),
        ("/a/path", "."),
        # Invalid paths for using with_name()
        ("/", "file.txt"),
        ("", "file.txt")
    ],
)
def test_with_name_errors(path, name) -> None:
    """Test that various forms of invalid .with_name() invocations are detected."""
    ws = mock_workspace_client()

    with pytest.raises(ValueError):
        _ = DbfsPath(path, ws=ws).with_name(name)


def test_with_stem() -> None:
    """Test that the stem in a path can be replaced."""
    ws = mock_workspace_client()

    assert DbfsPath("/dir/file.txt", ws=ws).with_stem("README") == DbfsPath("/dir/README.txt", ws=ws)


def test_with_suffix() -> None:
    """Test that the suffix of a path can be replaced, and that some errors are handled."""
    ws = mock_workspace_client()

    assert DbfsPath("/dir/README.txt", ws=ws).with_suffix(".md") == DbfsPath("/dir/README.md", ws=ws)
    with pytest.raises(ValueError, match="invalid suffix"):
        _ = DbfsPath("/dir/README.txt", ws=ws).with_suffix("txt")
    with pytest.raises(ValueError, match="empty name"):
        _ = DbfsPath("/", ws=ws).with_suffix(".txt")


def test_relative_to() -> None:
    """Test that it is possible to get the relative path between two paths."""
    ws = mock_workspace_client()

    # Basics.
    assert DbfsPath("/home/bob", ws=ws).relative_to("/") == DbfsPath("home/bob", ws=ws)
    assert DbfsPath("/home/bob", ws=ws).relative_to("/home") == DbfsPath("bob", ws=ws)
    assert DbfsPath("/home/bob", ws=ws).relative_to("/./home") == DbfsPath("bob", ws=ws)
    assert DbfsPath("foo/bar/baz", ws=ws).relative_to("foo") == DbfsPath("bar/baz", ws=ws)
    assert DbfsPath("foo/bar/baz", ws=ws).relative_to("foo/bar") == DbfsPath("baz", ws=ws)
    assert DbfsPath("foo/bar/baz", ws=ws).relative_to("foo/./bar") == DbfsPath("baz", ws=ws)

    # Walk-up (3.12+) behaviour.
    assert DbfsPath("/home/bob", ws=ws).relative_to("/usr", walk_up=True) == DbfsPath("../home/bob", ws=ws)

    # Check some errors.
    with pytest.raises(ValueError, match="different anchors"):
      _ = DbfsPath("/home/bob", ws=ws).relative_to("home")
    with pytest.raises(ValueError, match="not in the subpath"):
      _ = DbfsPath("/home/bob", ws=ws).relative_to("/usr")
    with pytest.raises(ValueError, match="cannot be walked"):
      _ = DbfsPath("/home/bob", ws=ws).relative_to("/home/../usr", walk_up=True)


@pytest.mark.parametrize(
    ("path", "parent"), [
        ("/foo/bar/baz", "/foo/bar"),
        ("/", "/"),
        (".", "."),
        ("foo/bar", "foo"),
        ("foo", ".")
    ]
)
def test_parent(path, parent) -> None:
    """Test that the parent of a path is properly calculated."""
    ws = mock_workspace_client()

    assert DbfsPath(path, ws=ws).parent == DbfsPath(parent, ws=ws)


@pytest.mark.parametrize(
    ("path", "parents"), [
        ("/foo/bar/baz", ("/foo/bar", "/foo", "/")),
        ("/", ()),
        (".", ()),
        ("foo/bar", ("foo", ".")),
        ("foo", (".",)),
    ]
)
def test_parents(path, parents) -> None:
    """Test that each of the parents of a path is returned."""
    ws = mock_workspace_client()

    expected_parents = tuple(DbfsPath(parent, ws=ws) for parent in parents)
    assert DbfsPath(path, ws=ws).parents == expected_parents


def test_is_relative_to() -> None:
    """Test detection of whether a path is relative to the target."""
    ws = mock_workspace_client()

    # Basics where it's true.
    assert DbfsPath("/home/bob", ws=ws).is_relative_to("/")
    assert DbfsPath("/home/bob", ws=ws).is_relative_to("/home")
    assert DbfsPath("/home/bob", ws=ws).is_relative_to("/./home")
    assert DbfsPath("foo/bar/baz", ws=ws).is_relative_to("foo")
    assert DbfsPath("foo/bar/baz", ws=ws).is_relative_to("foo/bar")
    assert DbfsPath("foo/bar/baz", ws=ws).is_relative_to("foo/./bar")

    # Some different situations where it isn't.
    assert not DbfsPath("/home/bob", ws=ws).is_relative_to("home")  # Different anchor.
    assert not DbfsPath("/home/bob", ws=ws).is_relative_to("/usr")  # Not a prefix.


def test_is_absolute() -> None:
    """Test detection of absolute versus relative paths."""
    ws = mock_workspace_client()

    assert DbfsPath("/foo/bar", ws=ws).is_absolute()
    assert DbfsPath("/", ws=ws).is_absolute()
    assert not DbfsPath("foo/bar", ws=ws).is_absolute()
    assert not DbfsPath(".", ws=ws).is_absolute()


def test_is_reserved() -> None:
    """Test detection of reserved paths (which aren't possible with DBFS)."""
    ws = mock_workspace_client()

    assert not DbfsPath("NUL", ws=ws).is_reserved()


def test_joinpath() -> None:
    """Test that paths can be joined."""
    ws = mock_workspace_client()

    assert DbfsPath("/home", ws=ws).joinpath("bob") == DbfsPath("/home/bob", ws=ws)
    assert DbfsPath("/home", ws=ws).joinpath(DbfsPath("bob", ws=ws)) == DbfsPath("/home/bob", ws=ws)
    assert DbfsPath("/home", ws=ws).joinpath(PurePosixPath("bob")) == DbfsPath("/home/bob", ws=ws)
    assert DbfsPath("/usr", ws=ws).joinpath("local", "bin") == DbfsPath("/usr/local/bin", ws=ws)
    assert DbfsPath("home", ws=ws).joinpath("jane") == DbfsPath("home/jane", ws=ws)


def test_match() -> None:
    """Test that glob matching works."""
    ws = mock_workspace_client()

    # Relative patterns, match from the right.
    assert DbfsPath("foo/bar/file.txt", ws=ws).match("*.txt")
    assert DbfsPath("/foo/bar/file.txt", ws=ws).match("bar/*.txt")
    assert not DbfsPath("/foo/bar/file.txt", ws=ws).match("foo/*.txt")

    # Absolute patterns, match from the left (and only against absolute paths)
    assert DbfsPath("/file.txt", ws=ws).match("/*.txt")
    assert not DbfsPath("foo/bar/file.txt", ws=ws).match("/*.txt")

    # Case-sensitive by default, but can be overridden.
    assert not DbfsPath("file.txt", ws=ws).match("*.TXT")
    assert DbfsPath("file.txt", ws=ws).match("*.TXT", case_sensitive=False)

    # No recursive globs.
    assert not DbfsPath("/foo/bar/file.txt", ws=ws).match("/**/*.txt")
