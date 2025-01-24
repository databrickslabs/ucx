from pathlib import Path

from databricks.labs.ucx.source_code.base import Advice, LocatedAdvice


def test_located_advice_message() -> None:
    advice = Advice(
        code="code",
        message="message",
        start_line=0,
        start_col=0,  # Zero based line number is incremented with one to create the message
        end_line=1,
        end_col=1,
    )
    located_advice = LocatedAdvice(advice, Path("test.py"))

    assert located_advice.message == "test.py:1:0: [code] message"
