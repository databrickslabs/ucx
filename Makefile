lint:
	@echo "Linting the project code"
	poetry run black .
	poetry run isort .
	poetry run ruff . --fix

verify:
	@echo "Verifying the project code"
	poetry run black . --check
	poetry run isort . --check
	poetry run ruff .
