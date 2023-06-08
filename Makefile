lint:
	@echo "Linting the project code"
	black .
	isort .
	ruff . --fix

verify:
	@echo "Verifying the project code"
	black . --check
	isort . --check
	ruff .
