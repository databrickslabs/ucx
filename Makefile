all: clean lint fmt test

clean:
	rm -fr htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	hatch env remove unit

dev:
	pip install hatch
	hatch env create
	hatch run pip install -e '.[test]'
	hatch run which python

lint:
	hatch run lint:verify

fmt:
	hatch run lint:fmt

test:
	hatch run unit:test

integration:
	hatch run integration:test

test-cov:
	hatch run unit:test-cov-report && open htmlcov/index.html

