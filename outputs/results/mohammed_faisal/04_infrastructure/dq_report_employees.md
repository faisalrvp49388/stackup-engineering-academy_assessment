# Data Quality Report — employees

_Generated 2026-09-21T17:42:49_

**Checks run:** 8 · **Passed:** 7 · **Failed:** 1

| Check | Status | Failed columns | Details |
|---|---|---|---|
| completeness | ✅ PASS | — | employee_id=100%, full_name=100%, email=100%, department=100%, role=100%, level=100%, hire_date=99%, salary=100%, manager_id=100%, region=100%, status=100%, years_experience=100%, dq_flags=3% |
| uniqueness | ✅ PASS | — | employee_id: 1000 unique / 1000 total (0 duplicates) |
| validity_numeric | ✅ PASS | — | all values within range |
| validity_date | ✅ PASS | — | all dates valid |
| consistency | ✅ PASS | — | all consistency rules hold |
| referential_integrity | ✅ PASS | — | all foreign keys resolve |
| distribution | ✅ PASS | — | no column exceeds 30% share |
| outliers | ❌ FAIL | salary | salary: 17 values beyond 3 std dev |
