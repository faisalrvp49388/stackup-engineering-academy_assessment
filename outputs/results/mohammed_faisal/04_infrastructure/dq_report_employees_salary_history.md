# Data Quality Report — employees_salary_history

_Generated 2026-09-21T17:42:49_

**Checks run:** 6 · **Passed:** 4 · **Failed:** 2

| Check | Status | Failed columns | Details |
|---|---|---|---|
| completeness | ✅ PASS | — | employee_id=100%, previous_salary=67%, new_salary=100%, previous_role=67%, new_role=100%, previous_level=67%, new_level=100%, effective_date=100%, change_type=100%, change_reason=100% |
| uniqueness | ❌ FAIL | employee_id, effective_date | employee_id+effective_date: 1825 unique / 1826 total (1 duplicates) |
| validity_numeric | ❌ FAIL | new_salary | new_salary: 100 below min (10000), 0 above max (100000) |
| validity_date | ✅ PASS | — | all dates valid |
| consistency | ✅ PASS | — | all consistency rules hold |
| referential_integrity | ✅ PASS | — | all foreign keys resolve |
