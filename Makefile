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

docs:
	cd docs && hugo server --buildDrafts --disableFastRender

docs-test-md:
	cd docs && hugo --minify && yarn linkinator 'content/**/*.md' --markdown --url-rewrite-search /images/ --url-rewrite-replace /static/images/

.PHONY: all clean dev lint fmt test integration coverage known solacc docs docs-test
