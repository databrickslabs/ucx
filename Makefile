all: clean lint fmt test coverage

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	hatch env create

dev: .venv/bin/python
	@hatch run which python

lint:
	hatch run verify

fmt:
	hatch run fmt

test:
	hatch run test

integration:
	hatch run integration

coverage:
	hatch run coverage && open htmlcov/index.html

known:
	hatch run python src/databricks/labs/ucx/source_code/known.py

.PHONY: all clean dev lint fmt test integration coverage known
