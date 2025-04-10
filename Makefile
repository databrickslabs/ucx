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
	hatch run coverage; status=$$?; [ -e "htmlcov/index.html" ] && open htmlcov/index.html; exit $$status

known:
	hatch run python src/databricks/labs/ucx/source_code/known.py

solacc:
	hatch run python tests/integration/source_code/solacc.py

.PHONY: all clean dev lint fmt test integration coverage known solacc

docs-install:
	yarn --cwd docs/ucx install

docs-serve-dev:
	yarn --cwd docs/ucx start

docs-build:
	yarn --cwd docs/ucx build

docs-serve: docs-build
	yarn --cwd docs/ucx serve
