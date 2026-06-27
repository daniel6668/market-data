# Task 3 Report: Utils Module

**Status:** ✅ Complete  
**Date:** 2026-06-27  
**Commit:** `dccca87` — feat: utils module — rate limiter, trading calendar, config loader

## Summary

Implemented `src/utils.py` with four components:
- **RateLimiter** — sliding window rate limiter with `.wait()` and `.remaining`
- **TradingCalendar** — simple trading day checker (weekend exclusion)
- **load_config()** — YAML config loader with sensible defaults
- **setup_logger()** — dual-handler logger (file + console)

## Test Results

```
tests/test_utils.py::test_rate_limiter_waits PASSED
tests/test_utils.py::test_rate_limiter_respects_burst PASSED
tests/test_utils.py::test_load_config PASSED
tests/test_utils.py::test_load_config_default PASSED

4 passed in 60.14s
```

- **Step 2 (expected FAIL):** `ModuleNotFoundError: No module named 'src.utils'` — confirmed.
- **Step 4 (expected PASS):** All 4 tests pass. `test_rate_limiter_waits` takes ~60s due to the intentional sleep when the 3rd call bursts past `max_calls=2` with `period=60`.

## Files Changed

| File | Action |
|------|--------|
| `src/utils.py` | Created (105 lines) |
| `tests/test_utils.py` | Created (33 lines) |

## Concerns / Notes

1. **test_rate_limiter_waits runtime:** The test sleeps ~60s because `period=60` and `max_calls=2` causes the 3rd call to wait for the oldest to expire. This is by design but makes the test suite slow. Consider mocking `time.sleep` in future iterations.

2. **TradingCalendar.is_trading_day:** Currently only checks weekends. The holiday month sets (`A_HOLIDAY_MONTHS`, etc.) are defined but not used in the logic — they're available for future enhancement.

3. **Recursive `wait()`:** The implementation uses recursion after sleeping. This is fine for the low call counts expected but could theoretically hit recursion limits if called in a tight loop with a tiny period.
