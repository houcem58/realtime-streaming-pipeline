## Summary

<!-- What does this PR change and why? -->

## Type of change

- [ ] Bug fix (drift detection regression, evaluation error)
- [ ] New dataset adapter
- [ ] New drift detection algorithm or check
- [ ] Performance improvement
- [ ] Documentation update
- [ ] CI / infrastructure change

## Benchmark impact

<!-- If this changes detection logic, run `make eval` and paste the results below -->

| Dataset | F1 before | F1 after | Change |
|---|---|---|---|
| HDFS | | | |
| NASA HTTP | | | |
| Online Retail II | | | |
| Olist | | | |

## Checklist

- [ ] `make test` passes
- [ ] `make lint` passes with no new violations
- [ ] If adding a dataset adapter: calibration thresholds documented in `docs/Calibration.md`
- [ ] If changing detection logic: ADR updated or new ADR created in `docs/decisions/`
- [ ] If changing Kafka topology: `docs/Architecture.md` updated
- [ ] Benchmark results table updated in README if F1 changed ≥ 0.01
