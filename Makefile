.PHONY: install test eval demo docker-up docker-down lint clean help

PYTHON := python
PIP := pip

help:
	@echo "Available targets:"
	@echo "  make install     Install dependencies"
	@echo "  make test        Run unit tests"
	@echo "  make lint        Run ruff linter"
	@echo "  make eval        Run full benchmark evaluation (requires datasets)"
	@echo "  make demo        Run end-to-end demo on HDFS sample data"
	@echo "  make docker-up   Start Kafka + Zookeeper"
	@echo "  make docker-down Stop Kafka + Zookeeper"
	@echo "  make clean       Remove Python cache files"

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m pytest evaluation/test_metrics.py -v

lint:
	$(PIP) install ruff --quiet
	ruff check streaming/ adapters/ evaluation/ kafka/ scripts/ --ignore E501

eval:
	$(PYTHON) scripts/eval_drift_detection.py
	$(PYTHON) scripts/eval_kafka_stream.py

demo:
	$(PYTHON) scripts/run_demo.py

docker-up:
	docker compose up -d
	@echo "Kafka ready. Run: python kafka/ensure_topics.py"

docker-down:
	docker compose down

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
