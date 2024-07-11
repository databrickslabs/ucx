from tests.unit.no_cheat import no_cheat


def test_no_cheat_returns_empty_string_for_empty_diff():
    diff_data = ""
    result = no_cheat(diff_data)
    assert not result


def test_no_cheat_returns_empty_string_for_no_cheat_diff():
    diff_data = """
+some code
-some other code
"""
    result = no_cheat(diff_data)
    assert not result


def test_no_cheat_returns_empty_string_for_removed_cheat():
    diff_data = """
+some code
-some other code # pylint: disable=some-rule
"""
    result = no_cheat(diff_data)
    assert not result


def test_no_cheat_returns_empty_string_for_replaced_cheat():
    diff_data = """
+some code # pylint: disable=some-rule
-some other code # pylint: disable=some-rule
"""
    result = no_cheat(diff_data)
    assert not result


def test_no_cheat_returns_message_for_single_cheat():
    diff_data = """
+some code # pylint: disable=some-rule
-some other code
"""
    result = no_cheat(diff_data)
    assert result == "Do not cheat the linter: found 1 additional # pylint: disable=some-rule"


def test_no_cheat_returns_message_for_multiple_same_cheat():
    diff_data = """
+some code # pylint: disable=some-rule
-some other code
+some code # pylint: disable=some-rule
"""
    result = no_cheat(diff_data)
    assert result == "Do not cheat the linter: found 2 additional # pylint: disable=some-rule"


def test_no_cheat_returns_message_for_multiple_cheats_in_different_lines():
    diff_data = """
+some code # pylint: disable=some-rule
-some other code
+some code # pylint: disable=some-other-rule
"""
    result = no_cheat(diff_data)
    assert result == (
        "Do not cheat the linter: found 1 additional # pylint: disable=some-rule\n"
        "Do not cheat the linter: found 1 additional # pylint: disable=some-other-rule"
    )


def test_no_cheat_returns_message_for_multiple_cheats_in_same_lines():
    diff_data = """
+some code # pylint: disable=some-rule, some-other-rule
-some other code
"""
    result = no_cheat(diff_data)
    assert result == (
        "Do not cheat the linter: found 1 additional # pylint: disable=some-rule\n"
        "Do not cheat the linter: found 1 additional # pylint: disable=some-other-rule"
    )
