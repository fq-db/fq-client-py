.PHONY: lint
lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

.PHONY: test
test:
	uv run pytest -m "not integration"

.PHONY: test-integration
test-integration:
	uv run pytest -m integration
