# Contributing

Thank you for your interest in contributing to the Real-Time Schema Drift Detection Pipeline.

## Ways to Contribute

- **Bug reports** — open an issue with steps to reproduce and expected vs actual behavior
- **New dataset adapters** — add a normalizer in `adapters/` and update `eval_kafka_stream.py`
- **Additional drift types** — extend `DriftDetector` with a new detection method + test
- **Calibration improvements** — per-dataset threshold research with documented rationale
- **Documentation** — improve any doc under `docs/` or the main README

## Development Setup

```bash
git clone https://github.com/houcem58/realtime-streaming-pipeline.git
cd realtime-streaming-pipeline
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install pre-commit
pre-commit install
```

## Running Tests

```bash
make test           # pytest unit tests
make eval           # full benchmark (requires datasets)
```

## Code Standards

- All new code must have type hints
- All new classes and public methods must have docstrings
- No print statements — use proper return values or structured output
- `ruff check` must pass with no ignored rules (except E501 for long strings)
- Tests for any new drift detection method are required before PR review

## Adding a Dataset Adapter

1. Create `adapters/your_dataset_adapter.py` implementing `normalize_events() -> pd.DataFrame`
2. The output DataFrame must conform to `data/schema.json` (20-column normalized schema)
3. Add a `DriftInjector` configuration for your dataset in `evaluation/drift_injector.py`
4. Add an evaluation entry in `scripts/eval_kafka_stream.py`
5. Document calibration decisions in `docs/Calibration.md`

## Pull Request Process

1. Branch from `main` with a descriptive name (`feat/olist-adapter`, `fix/binary-fpr`)
2. Keep commits atomic and descriptive
3. Update `CHANGELOG.md` under `[Unreleased]` with your change
4. All CI checks must pass — lint, test, integration
5. PRs require one reviewer approval before merge

## Reporting Issues

When filing a bug:
- Python version and OS
- Exact command that produced the error
- Full traceback (not just the last line)
- Dataset name and approximate size if relevant

## Code of Conduct

Be professional. Focus on technical merit. Personal criticism is not welcome.
