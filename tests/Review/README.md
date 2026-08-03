# Code-review verification tests

`tests/Review` contains characterization tests for disputed review findings. They are deliberately outside the default PHPUnit suites because they assert the current undesirable behavior in order to prove that the issue is reproducible.

Run them with:

```bash
composer review
```

A passing characterization test means the named issue was reproduced. When production code fixes an issue, invert the assertion to the desired behavior and move the test into the appropriate `tests/Unit` or `tests/Integration` directory.

Candidate performance comparisons live beside the characterization tests and use test-local implementations without modifying production code. Run them with:

```bash
composer review:bench
```

Benchmark results are environment-specific. Use the direction and relative magnitude of repeated runs rather than treating one absolute microsecond value as a release guarantee.
