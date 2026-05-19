.PHONY: data test lint bundle-validate

data:
	python scripts/generate_synthetic_data.py --output-dir data/synthetic --days 10 --seed 44

test:
	pytest -q

lint:
	ruff check src tests scripts

bundle-validate:
	databricks bundle validate -t dev
