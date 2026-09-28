-- =============================================================
-- Presight — Task 2.1: six business questions
-- Runs on the warehouse built by 01_foundations/data_model.sql
-- (python 02_sql_and_viz/run_sql.py builds it, then runs this file).
-- =============================================================

-- ===========================================================================
-- SECTION 3 — TASK 2.1: Six business questions
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Q1. DEPARTMENT BUDGET PERFORMANCE
-- Approach: aggregate dim_project per department, compute spend % = actual/budget
-- and keep departments above 90% (this includes every over-budget department,
-- because >100% is also >90%). NULLIF guards against a zero budget.
-- ---------------------------------------------------------------------------
SELECT department,
       SUM(budget)                                                   AS total_budget,
       SUM(actual_cost)                                              AS total_actual_cost,
       ROUND(100.0 * SUM(actual_cost) / NULLIF(SUM(budget), 0), 2)   AS spend_percentage,
       SUM(actual_cost) > SUM(budget)                                AS over_budget
FROM dim_project
GROUP BY department
HAVING 100.0 * SUM(actual_cost) / NULLIF(SUM(budget), 0) > 90
    OR SUM(actual_cost) > SUM(budget)
ORDER BY spend_percentage DESC;

-- ---------------------------------------------------------------------------
-- Q2. PROJECT MANAGER WORKLOAD (current employee data)
-- Approach: "active" = status 'In Progress'. Join projects to the CURRENT version
-- of the manager (is_current = TRUE) so each manager appears once with today's
-- name/email, and keep managers with more than three active projects.
-- RESULT NOTE: the busiest managers currently hold exactly 3 In Progress projects, so on this
-- dataset "more than three" legitimately returns 0 rows (use >= 3 to see them).
-- ---------------------------------------------------------------------------
SELECT e.full_name, e.email,
       COUNT(*)            AS active_project_count,
       SUM(p.budget)       AS combined_budget_responsibility,
       SUM(p.actual_cost)  AS combined_actual_spend
FROM dim_project p
JOIN dim_employee e ON e.employee_id = p.project_manager_id AND e.is_current = TRUE
WHERE p.status = 'In Progress'
GROUP BY e.employee_id, e.full_name, e.email
HAVING COUNT(*) > 3
ORDER BY active_project_count DESC, combined_budget_responsibility DESC;

-- ---------------------------------------------------------------------------
-- Q3. VENDOR CONCENTRATION RISK
-- Approach: vendor spend / total spend using a window SUM over the grouped rows
-- (one pass, no second scan of the fact table). Keeps vendors above 5% and
-- assigns HIGH (>10%), MEDIUM (5-10%).
-- RESULT NOTE: 25 vendors of similar size; the largest share is 4.36%, so on this dataset the
-- query legitimately returns 0 rows (no concentration risk). Use > 4 to see the output shape.
-- ---------------------------------------------------------------------------
WITH vendor_spend AS (
    SELECT v.vendor_name, SUM(f.amount) AS total_spend, COUNT(*) AS transaction_count
    FROM fact_transactions f
    JOIN dim_vendor v USING (vendor_key)
    GROUP BY v.vendor_name
)
SELECT vendor_name, total_spend, transaction_count,
       ROUND(100.0 * total_spend / SUM(total_spend) OVER (), 2) AS percentage_of_total_spend,
       CASE WHEN 100.0 * total_spend / SUM(total_spend) OVER () > 10 THEN 'HIGH'
            WHEN 100.0 * total_spend / SUM(total_spend) OVER () >= 5 THEN 'MEDIUM'
            ELSE 'NORMAL' END AS risk_flag
FROM vendor_spend
QUALIFY 100.0 * total_spend / SUM(total_spend) OVER () > 5     -- DuckDB; in Postgres wrap in a subquery
ORDER BY percentage_of_total_spend DESC;

-- ---------------------------------------------------------------------------
-- Q4. PROJECTS WITH OPEN FINANCIAL ISSUES
-- Approach: filter early to Pending/Disputed, aggregate per project, then apply
-- the > 50,000 AED threshold with HAVING.
-- ---------------------------------------------------------------------------
SELECT p.project_id, p.project_name, p.department, p.status AS project_status,
       COUNT(*)      AS open_transaction_count,
       SUM(f.amount) AS open_transaction_value
FROM fact_transactions f
JOIN dim_project p USING (project_key)
WHERE f.payment_status IN ('Pending', 'Disputed')
GROUP BY p.project_id, p.project_name, p.department, p.status
HAVING SUM(f.amount) > 50000
ORDER BY open_transaction_value DESC;

-- ---------------------------------------------------------------------------
-- Q5. MONTHLY SPEND TREND WITH RUNNING TOTAL
-- Approach: aggregate to (month, category) first, then window functions over the
-- aggregate: running SUM partitioned by category ordered by month, and LAG for
-- the month-over-month % change (NULL for a category's first month or when the
-- previous month is 0). Months without spend in a category have no row, so LAG
-- compares against the previous month that HAD spend.
-- ---------------------------------------------------------------------------
WITH monthly AS (
    SELECT d.year_month, f.category, SUM(f.amount) AS monthly_spend
    FROM fact_transactions f
    JOIN dim_date d USING (date_key)
    GROUP BY d.year_month, f.category
)
SELECT year_month, category, monthly_spend,
       SUM(monthly_spend) OVER (PARTITION BY category ORDER BY year_month
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       ROUND(100.0 * (monthly_spend - LAG(monthly_spend) OVER w)
             / NULLIF(LAG(monthly_spend) OVER w, 0), 2) AS month_over_month_pct_change
FROM monthly
WINDOW w AS (PARTITION BY category ORDER BY year_month)
ORDER BY category, year_month;

-- ---------------------------------------------------------------------------
-- Q6. LARGEST SINGLE SALARY INCREASES (SCD2 self-join)
-- Approach: join each dim_employee version (prev) to the version that starts
-- exactly when it ends (next.valid_from = prev.valid_to) for the same employee.
-- The pair gives previous vs new salary; keep true increases, rank by AED amount.
-- ---------------------------------------------------------------------------
SELECT n.employee_id, n.full_name, n.valid_from AS change_date,
       p.salary AS previous_salary, n.salary AS new_salary,
       n.salary - p.salary                                AS increase_amount,
       ROUND(100.0 * (n.salary - p.salary) / p.salary, 2) AS increase_pct
FROM dim_employee p
JOIN dim_employee n ON n.employee_id = p.employee_id AND n.valid_from = p.valid_to
WHERE n.salary > p.salary
ORDER BY increase_amount DESC, n.employee_id
LIMIT 20;


