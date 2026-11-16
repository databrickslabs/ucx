all: clean dev fmt test

# Ensure that all uv commands don't automatically update the lock file: instead they use the locked dependencies.
export UV_FROZEN := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 4 --timeout 30 --durations 20

clean:
	rm -fr .venv htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
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

# The app frontend is a static export, built into the Python package so that it ships in the wheel.
APP_UI_STATIC := src/databricks/labs/ucx/app/static

app-ui:
	cd app-ui && pnpm install --frozen-lockfile && NEXT_TELEMETRY_DISABLED=1 pnpm build
	rm -rf $(APP_UI_STATIC)
	cp -R app-ui/out $(APP_UI_STATIC)

# Hashed requirements of the app, from uv.lock: `deploy-app` installs exactly these in the app environment.
APP_REQUIREMENTS := src/databricks/labs/ucx/app/bundle/requirements.txt

app-requirements:
	uv export --frozen --extra app --no-default-groups --no-emit-project --no-header --no-annotate \
		--format requirements-txt --output-file $(APP_REQUIREMENTS)

build: app-ui app-requirements
	uv build --require-hashes --build-constraints=.build-constraints.txt

lock-dependencies: UV_FROZEN := 0
lock-dependencies:
	uv lock --upgrade
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	    uv pip compile --upgrade --generate-hashes --universal --no-header --quiet - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt
	@perl -pi \
		-e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g;' \
		-e 's|url = "https://[^"]*/packages/([^"]*)"|url = "https://files.pythonhosted.org/packages/$$1"|g;' \
		uv.lock
	@printf 'Stripped registry references from uv.lock.\n'

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

.DEFAULT: all
.PHONY: all clean dev lint fmt test integration coverage app-ui app-requirements build lock-dependencies known cmds solacc docs-install docs-serve-dev docs-build docs-serve
