# Data Quality Report — transactions

_Generated 2026-09-24T15:31:33_

**Checks run:** 9 · **Passed:** 7 · **Failed:** 2

| Check | Status | Failed columns | Details |
|---|---|---|---|
| completeness | ✅ PASS | — | transaction_id=100%, project_id=100%, vendor_id=100%, vendor_name=100%, category=100%, amount=99%, currency=100%, transaction_date=100%, approved_by=95%, payment_status=100%, invoice_ref=100%, notes=78%, project_name=100%, department=100%, approver_name=95%, is_approved=100%, amount_aed=100%, transaction_year_month=100% |
| uniqueness | ✅ PASS | — | transaction_id: 50000 unique / 50000 total (0 duplicates) |
| validity_numeric | ✅ PASS | — | all values within range |
| validity_date | ✅ PASS | — | all dates valid |
| consistency | ✅ PASS | — | all consistency rules hold |
| referential_integrity | ✅ PASS | — | all foreign keys resolve |
| distribution | ✅ PASS | — | no column exceeds 30% share |
| freshness | ❌ FAIL | transaction_date | latest transaction_date = 2026-08-04 (51 days old) |
| outliers | ❌ FAIL | amount | amount: 1593 values beyond 3 std dev |
