clean:
	rm -fr htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml

lint:
	hatch run lint:verify

fmt:
	hatch run lint:fmt

test:
	hatch run unit:test

test-cov:
	hatch run unit:test-cov-report && open htmlcov/index.html