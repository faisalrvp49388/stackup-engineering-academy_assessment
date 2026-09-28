# Data Quality Report — projects

_Generated 2026-09-24T15:31:33_

**Checks run:** 8 · **Passed:** 6 · **Failed:** 2

| Check | Status | Failed columns | Details |
|---|---|---|---|
| completeness | ❌ FAIL | start_date | project_id=100%, project_name=100%, department=100%, status=100%, start_date=87%, end_date=43%, budget=100%, actual_cost=100%, project_manager_id=100%, priority=100%, region=100%, budget_variance=88%, is_over_budget=100%, duration_days=43%, budget_utilisation_pct=88%, status_category=100%, risk_level=100% |
| uniqueness | ✅ PASS | — | project_id: 500 unique / 500 total (0 duplicates) |
| validity_numeric | ✅ PASS | — | all values within range |
| validity_date | ✅ PASS | — | all dates valid |
| consistency | ✅ PASS | — | all consistency rules hold |
| referential_integrity | ✅ PASS | — | all foreign keys resolve |
| distribution | ✅ PASS | — | no column exceeds 30% share |
| outliers | ❌ FAIL | actual_cost | actual_cost: 11 values beyond 3 std dev |
