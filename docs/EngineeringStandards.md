# Engineering Standards — Realtime Streaming Pipeline

These standards apply to all contributions. They encode the team's working agreements,
not general best practices. For context on why these choices were made, see `docs/decisions/`.

---

## Code Style

- **Formatter:** ruff (line length: 120)
- **Imports:** standard library → third-party → local, separated by blank lines
- **Type annotations:** required on all public function signatures
- **Docstrings:** required on all classes and public methods; one short line unless the WHY
  is non-obvious. Never summarize what the function name already says.

```python
# Acceptable
def detect(self, batch: list[dict]) -> list[DriftEvent]:
    """Run all 5 drift checks against the fitted baseline."""

# Not acceptable — repeats the function name
def detect(self, batch):
    """This function detects drift in a batch of events."""
```

---

## Drift Detection Standards

### Adding a new drift check

Every new drift check must:
1. Be a private method `_check_<name>(self, batch) -> list[DriftEvent]`
2. Return an empty list (not None) if no drift detected
3. Include a `DriftEvent` with `severity`, `column`, `value`, `threshold`, and `action` fields
4. Have a corresponding test in `evaluation/test_metrics.py` (or a new test file)
5. Be documented in `docs/Architecture.md` component responsibilities
6. Have its threshold derivation documented in `docs/Calibration.md`

### Threshold policy

**No magic numbers in detection logic.** Every threshold must be either:
- Loaded from a `DriftConfig` instance (per-dataset configuration), or
- Derived from the baseline (e.g., `mean + k * sigma`)

If you hardcode a threshold without configuration, the PR will be rejected.

---

## Dataset Adapter Standards

Every adapter must:
1. Inherit from `BaseAdapter`
2. Implement `normalize(row: dict) -> dict` returning exactly the 20-column event schema
3. Handle missing or malformed columns gracefully (return default values, not raise)
4. Include a calibration entry in `docs/Calibration.md` before merging

The 20-column event schema is frozen. Adding columns requires an ADR.

---

## Evaluation Standards

### Benchmark reporting

When making a change that affects detection logic, the PR must include:
- F1 before and after for all 4 datasets
- The command used to reproduce the results
- If F1 drops > 0.02 on any dataset, an explanation in the PR description

### Reproducibility

All benchmark results must be reproducible with `seed=42`. If your change requires a different
seed or produces non-deterministic results, this must be justified in an ADR.

---

## Testing Standards

- Unit tests in `evaluation/test_*.py`
- Test file must be named `test_<module>.py`
- Each public class or function must have at least:
  - One happy-path test
  - One edge case test (empty input, zero values, boundary conditions)
- Tests must not require Kafka or external services
- `make test` must pass locally before opening a PR

---

## Commit Message Standards

```
<type>(<scope>): <short summary>

[optional body — explain WHY not WHAT]
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `ci`, `chore`

Examples:
```
feat(detector): add schema_rename check for column set changes

Detects when the incoming batch has a different set of column names
than the baseline. Fires at severity=HIGH regardless of threshold config
because schema changes are always actionable.

fix(retail-adapter): handle missing StockCode in Online Retail II

Some rows in the raw CSV have empty StockCode; previous code raised
KeyError. Now defaults to "UNKNOWN" to preserve the event.

docs(calibration): add HDFS threshold derivation
```

---

## PR Standards

- Use the PR template (populated automatically when opening a PR)
- Self-review before requesting review — check the template checklist
- All benchmark checks must be green before requesting review
- One approval required to merge (even for solo projects — use the review process to document decisions)

---

## CI Requirements

The pipeline has 3 jobs: `lint`, `test`, `validate-data`. All must pass before merge.

`validate-data` runs `make eval` in fast mode (reduced dataset sample). Full benchmark
is run manually and results committed to `docs/` if significant.

---

## Documentation Standards

When your PR adds a feature:
- Update `docs/Architecture.md` if you changed component responsibilities
- Update `docs/Calibration.md` if you changed thresholds
- Update `CHANGELOG.md` under `[Unreleased]`
- Create or update an ADR in `docs/decisions/` if you made a non-obvious architectural choice

When your PR fixes a bug:
- Add a test that reproduces the bug before the fix
- Optionally update `docs/Runbook.md` with the failure mode and recovery
