all: clean lint fmt test coverage

# Ensure that all uv commands don't automatically update the lock file. If UV_FROZEN=1 (from the environment)
# then UV_LOCKED should _not_ be set, but otherwise it needs to be set to ensure the lock-file is only ever
# deliberately updated.
ifneq ($(UV_FROZEN),1)
export UV_LOCKED := 1
endif
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 4 --timeout 30 --durations 20

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

lint:
	$(UV_RUN) black --check . --extend-exclude 'tests/unit/source_code/samples/'
	$(UV_RUN) ruff check .
	$(UV_RUN) mypy --exclude 'tests/unit/source_code/samples/*' --exclude dist .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

fmt:
	$(UV_RUN) black . --extend-exclude 'tests/unit/source_code/samples/'
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) mypy --disable-error-code 'annotation-unchecked' --exclude 'tests/unit/source_code/samples/*' --exclude dist .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

test:
	$(UV_TEST) --cov src --cov-report=xml tests/unit --cov-fail-under=89

integration:
	$(UV_TEST) --cov src --cov-report=xml tests/integration

coverage:
	$(UV_TEST) --cov src tests/unit --cov-report=html --cov-fail-under=89
	open htmlcov/index.html

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

lock-dependencies: UV_LOCKED := 0
lock-dependencies:
	uv lock
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	    uv pip compile --generate-hashes --universal --no-header - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt

known:
	$(UV_RUN) python src/databricks/labs/ucx/source_code/known.py

cmds:
	$(UV_RUN) python tests/integration/cmds.py

solacc:
	$(UV_RUN) python tests/integration/source_code/solacc.py

docs-install:
	yarn --cwd docs/ucx install

docs-serve-dev:
	yarn --cwd docs/ucx start

docs-build:
	yarn --cwd docs/ucx build

docs-serve: docs-build
	yarn --cwd docs/ucx serve

.PHONY: all clean dev lint fmt test integration coverage build lock-dependencies known cmds solacc docs-install docs-serve-dev docs-build docs-serve
