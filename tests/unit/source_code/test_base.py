from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Convention,
    Deprecation,
    Failure,
)


def test_message_initialization():
    message = Advice('code1', 'This is a message', "LOCAL_FILE", "some path", 1, 1, 2, 2)
    assert message.code == 'code1'
    assert message.message == 'This is a message'
    assert message.location_type == 'LOCAL_FILE'
    assert message.location_path == 'some path'
    assert message.start_line == 1
    assert message.start_col == 1
    assert message.end_line == 2
    assert message.end_col == 2


def test_warning_initialization():
    warning = Advisory('code2', 'This is a warning', "LOCAL_FILE", "some path", 1, 1, 2, 2)

    copy_of = warning.replace(code='code3')
    assert copy_of.code == 'code3'
    assert isinstance(copy_of, Advisory)


def test_error_initialization():
    error = Failure('code3', 'This is an error', "LOCAL_FILE", "some path", 1, 1, 2, 2)
    assert isinstance(error, Advice)


def test_deprecation_initialization():
    deprecation = Deprecation('code4', 'This is a deprecation', "LOCAL_FILE", "some path", 1, 1, 2, 2)
    assert isinstance(deprecation, Advice)


def test_convention_initialization():
    convention = Convention('code5', 'This is a convention', "LOCAL_FILE", "some path", 1, 1, 2, 2)
    assert isinstance(convention, Advice)
