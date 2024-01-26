from databricks.labs.ucx.framework.utils import escape_sql_identifier


def test_escaped_path():
    # test with optional escaping
    assert "a" == escape_sql_identifier("a")
    assert "a.b" == escape_sql_identifier("a.b")
    assert "a.b.c" == escape_sql_identifier("a.b.c")
    assert "a" == escape_sql_identifier("a")
    assert "a.b" == escape_sql_identifier("a.b")
    assert "`a`.`b`.`c`" == escape_sql_identifier("`a`.`b`.`c`")
    assert "`a.b`.`c`" == escape_sql_identifier("`a.b`.`c`")
    assert "`a-b`.c.d" == escape_sql_identifier("a-b.c.d")
    assert "a.`b-c`.d" == escape_sql_identifier("a.b-c.d")
    assert "a.b.`c-d`" == escape_sql_identifier("a.b.c-d")
    assert "`✨`.`🍰`.`✨`" == escape_sql_identifier("✨.🍰.✨")
    assert "a.b.`c.d`" == escape_sql_identifier("a.b.c.d")
    # test with escaping enforced
    assert "`a`" == escape_sql_identifier("a", False)
    assert "`a`.`b`" == escape_sql_identifier("a.b", False)
    assert "`a`.`b`.`c`" == escape_sql_identifier("a.b.c", False)
    assert "`a-b`.`c`.`d`" == escape_sql_identifier("a-b.c.d", False)
    assert "`a`.`b-c`.`d`" == escape_sql_identifier("a.b-c.d", False)
    assert "`a`.`b`.`c-d`" == escape_sql_identifier("a.b.c-d", False)
    assert "`a`.`b`.`c.d`" == escape_sql_identifier("a.b.c.d", False)
