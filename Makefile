.PHONY: test coverage clean

test:
	uv run pytest tests/

coverage:
	uv run pytest --cov=migraine_weather --cov-report=term-missing --cov-report=html tests/
