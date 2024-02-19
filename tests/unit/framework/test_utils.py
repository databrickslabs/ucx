from databricks.labs.ucx.framework.utils import escape_sql_identifier


def test_escaped_path():
    # test with optional escaping
    assert escape_sql_identifier("a") == "a"
    assert escape_sql_identifier("a.b") == "a.b"
    assert escape_sql_identifier("a.b.c") == "a.b.c"
    assert escape_sql_identifier("a") == "a"
    assert escape_sql_identifier("a.b") == "a.b"
    assert escape_sql_identifier("`a`.`b`.`c`") == "`a`.`b`.`c`"
    assert escape_sql_identifier("`a.b`.`c`") == "`a.b`.`c`"
    assert escape_sql_identifier("a-b.c.d") == "`a-b`.c.d"
    assert escape_sql_identifier("a.b-c.d") == "a.`b-c`.d"
    assert escape_sql_identifier("a.b.c-d") == "a.b.`c-d`"
    assert escape_sql_identifier("✨.🍰.✨") == "`✨`.`🍰`.`✨`"
    assert escape_sql_identifier("a.b.c.d") == "a.b.`c.d`"
    # test with escaping enforced
    assert escape_sql_identifier("a", False) == "`a`"
    assert escape_sql_identifier("a.b", False) == "`a`.`b`"
    assert escape_sql_identifier("a.b.c", False) == "`a`.`b`.`c`"
    assert escape_sql_identifier("a-b.c.d", False) == "`a-b`.`c`.`d`"
    assert escape_sql_identifier("a.b-c.d", False) == "`a`.`b-c`.`d`"
    assert escape_sql_identifier("a.b.c-d", False) == "`a`.`b`.`c-d`"
    assert escape_sql_identifier("a.b.c.d", False) == "`a`.`b`.`c.d`"
