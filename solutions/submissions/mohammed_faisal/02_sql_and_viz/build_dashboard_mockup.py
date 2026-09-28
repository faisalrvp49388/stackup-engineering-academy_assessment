"""
Task 2.4 — executive dashboard mockup (PDF) built from the real warehouse numbers.

  python solutions/submissions/mohammed_faisal/02_sql_and_viz/build_dashboard_mockup.py

Reads outputs/presight_warehouse.duckdb (run run_sql.py first) and writes
outputs/results/mohammed_faisal/02_sql_and_viz/dashboard_mockup.pdf
"""
import os

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(HERE))))
DB = os.path.join(ROOT, "outputs", "presight_warehouse.duckdb")
OUT = os.path.join(ROOT, "outputs", "results", "mohammed_faisal", "02_sql_and_viz", "dashboard_mockup.pdf")

# Colour palette: dark text, muted labels, blue for actuals, orange for warnings.
INK, MUTED, BLUE, ORANGE, GREY = "#1f2937", "#6b7280", "#2563eb", "#ea580c", "#d1d5db"
# Categorical colours for lines and donut slices (grey is reserved for 'Other').
PALETTE = ["#2563eb", "#0891b2", "#7c3aed", "#ea580c", "#16a34a", "#9ca3af"]

# Read-only connection to the warehouse built by run_sql.py.
con = duckdb.connect(DB, read_only=True)
# Small helper: run SQL and return a DataFrame.
q = lambda sql: con.execute(sql).fetchdf()  # noqa: E731

# KPI cards: total budget, actual spend and the share of over-budget projects.
kpi = q("""SELECT SUM(budget) b, SUM(actual_cost) a,
                  100.0*AVG(CASE WHEN is_over_budget THEN 1 ELSE 0 END) over_pct FROM dim_project""").iloc[0]
n_txn = q("SELECT COUNT(*) n FROM fact_transactions").iloc[0, 0]
# Data for the department bar chart (Q1).
dept = q("""SELECT department, SUM(budget) b, SUM(actual_cost) a, 100.0*SUM(actual_cost)/SUM(budget) pct
            FROM dim_project GROUP BY 1 ORDER BY pct DESC""")
# Data for the monthly spend line chart (Q5).
trend = q("""SELECT d.year_month ym, f.category, SUM(f.amount) spend FROM fact_transactions f
             JOIN dim_date d USING (date_key) GROUP BY 1,2 ORDER BY 1""")
top_cat = trend.groupby("category").spend.sum().nlargest(5).index.tolist()
# Data for the vendor donut (Q3).
vend = q("""SELECT v.vendor_name, SUM(f.amount) spend FROM fact_transactions f JOIN dim_vendor v USING (vendor_key)
            GROUP BY 1 ORDER BY 2 DESC""")
# Data for the top-10 budget variance table.
top10 = q("""SELECT project_id, project_name, department, budget, actual_cost, budget_variance
             FROM dim_project WHERE budget_variance IS NOT NULL ORDER BY budget_variance DESC LIMIT 10""")

# Page 1: the dashboard itself (A3 landscape).
fig = plt.figure(figsize=(16.5, 11.7))
fig.patch.set_facecolor("white")
fig.text(0.03, 0.965, "Presight — Project Spend Performance", fontsize=22, fontweight="bold", color=INK)
fig.text(0.03, 0.94, "Author: Mohammed Faisal  ·  2026-09-21  ·  Source: presight_warehouse.duckdb (500 projects, 50,000 transactions)",
         fontsize=10.5, color=MUTED)

# Slicers (mock)
for i, (label, val) in enumerate([("Region", "All"), ("Project status", "All"), ("Year", "2022-2026")]):
    fig.text(0.62 + i * 0.125, 0.955, label.upper(), fontsize=8, color=MUTED)
    fig.text(0.62 + i * 0.125, 0.935, f"[ {val}  ▾ ]", fontsize=11, color=INK,
             bbox=dict(boxstyle="round,pad=0.3", fc="#f3f4f6", ec=GREY))

# KPI cards
# Four KPI cards along the top.
cards = [("Total budget", f"AED {kpi.b / 1e6:,.1f}M"), ("Total actual spend", f"AED {kpi.a / 1e6:,.1f}M"),
         ("Over-budget projects", f"{kpi.over_pct:.1f}%"), ("Total transactions", f"{n_txn:,}")]
for i, (t, v) in enumerate(cards):
    ax = fig.add_axes([0.03 + i * 0.2415, 0.845, 0.225, 0.075])
    ax.set_facecolor("#f9fafb"); ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_color(GREY)
    ax.text(0.05, 0.68, t, fontsize=10, color=MUTED, transform=ax.transAxes)
    ax.text(0.05, 0.18, v, fontsize=20, fontweight="bold", color=INK, transform=ax.transAxes)

# Bar: budget vs actual by department (Q1)
# Horizontal bars: budget vs actual per department, sorted by spend %.
ax = fig.add_axes([0.09, 0.47, 0.38, 0.32])
d = dept.sort_values("pct")
y = range(len(d))
ax.barh([i + 0.2 for i in y], d.b / 1e6, height=0.38, color=GREY, label="Budget")
ax.barh([i - 0.2 for i in y], d.a / 1e6, height=0.38, color=BLUE, label="Actual spend")
ax.set_yticks(list(y)); ax.set_yticklabels(d.department, fontsize=9)
ax.set_xlabel("AED millions", fontsize=9)
ax.set_title("Actual spend vs budget by department (Q1)", loc="left", fontsize=12, fontweight="bold", color=INK)
ax.legend(frameon=False, fontsize=9, loc="lower right")
for s in ("top", "right"):
    ax.spines[s].set_visible(False)

# Line: monthly spend by category (Q5)
# Line chart: monthly spend of the five largest categories.
ax = fig.add_axes([0.55, 0.47, 0.42, 0.32])
for c, col in zip(top_cat, PALETTE):
    t = trend[trend.category == c]
    ax.plot(t.ym, t.spend / 1e6, label=c, color=col, lw=1.8)
ticks = sorted(trend.ym.unique())[::6]
ax.set_xticks(ticks); ax.set_xticklabels(ticks, rotation=45, fontsize=8)
ax.set_ylabel("AED millions", fontsize=9)
ax.set_title("Monthly spend trend — top 5 categories (Q5)", loc="left", fontsize=12, fontweight="bold", color=INK)
ax.legend(frameon=False, fontsize=8, ncol=2)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)

# Table: top 10 projects by variance
ax = fig.add_axes([0.03, 0.05, 0.55, 0.36]); ax.axis("off")
ax.set_title("Top 10 projects by budget variance (projects_clean)", loc="left", fontsize=12,
             fontweight="bold", color=INK)
rows = [[r.project_id, r.project_name[:34], r.department, f"{r.budget:,.0f}", f"{r.actual_cost:,.0f}",
         f"+{r.budget_variance:,.0f}"] for r in top10.itertuples()]
# Table: exact values for the ten worst budget overruns.
tb = ax.table(cellText=rows, colLabels=["Project", "Name", "Dept", "Budget", "Actual", "Variance (AED)"],
              loc="upper center", cellLoc="left", colWidths=[.09, .32, .15, .12, .12, .16])
tb.auto_set_font_size(False); tb.set_fontsize(8.5); tb.scale(1, 1.55)
for (r, c), cell in tb.get_celld().items():
    cell.set_edgecolor(GREY)
    if r == 0:
        cell.set_facecolor("#e5e7eb"); cell.set_text_props(fontweight="bold")
    elif c == 5:
        cell.set_text_props(color=ORANGE, fontweight="bold")

# Donut: vendor concentration (Q3)
ax = fig.add_axes([0.63, 0.06, 0.33, 0.34])
# Donut: top five vendors plus everything else grouped as 'Other'.
top5 = vend.head(5)
vals = list(top5.spend) + [vend.spend.iloc[5:].sum()]
labels = list(top5.vendor_name) + ["Other (20 vendors)"]
ax.pie(vals, labels=labels, colors=PALETTE, startangle=90, wedgeprops=dict(width=0.38, edgecolor="white"),
       autopct=lambda p: f"{p:.1f}%", pctdistance=0.81, textprops=dict(fontsize=8))
ax.set_title("Vendor concentration — top 5 + Other (Q3)", fontsize=12, fontweight="bold", color=INK)
ax.text(0, 0, "no vendor\n> 5%", ha="center", va="center", fontsize=10, color=MUTED)

fig.text(0.03, 0.012,
         "Chart choices: bars compare two measures per category · line shows trend over time · table gives exact values for action · "
         "donut shows share of total (only 6 slices) · KPI cards for headline numbers · slicers filter every visual.",
         fontsize=8, color=MUTED)

os.makedirs(os.path.dirname(OUT), exist_ok=True)
# Write a two-page PDF: the dashboard and a page explaining each visual.
with PdfPages(OUT) as pdf:
    pdf.savefig(fig)
    # Page 2 — annotations
    f2 = plt.figure(figsize=(16.5, 11.7))
    f2.text(0.05, 0.94, "Dashboard annotations — what each visual shows and why", fontsize=18, fontweight="bold")
    notes = [
        ("KPI cards", "Headline totals from projects_clean / fact_transactions. Cards give a 2-second health check "
                      f"(budget AED {kpi.b/1e6:,.1f}M vs spend AED {kpi.a/1e6:,.1f}M, {kpi.over_pct:.1f}% of projects over budget)."),
        ("Horizontal bar — spend vs budget by department (Q1)", "Paired bars make the gap between two measures visible; "
                      "horizontal layout fits 12 long department names and is sorted by spend %. Legal is highest at 90.45%."),
        ("Line chart — monthly spend by category (Q5)", "A line is the right form for change over time; limited to the 5 largest "
                      "categories to stay readable (all 12 are available via a category slicer)."),
        ("Table — top 10 projects by budget variance", "Tables suit exact values and drill-down; conditional colour on the "
                      "variance column points the finance team at the worst overruns first."),
        ("Donut — vendor concentration (Q3)", "Share-of-total for part-to-whole with a small number of slices (top 5 + Other). "
                      "The centre note records the Q3 finding: the largest vendor holds 4.36%, so none breaches the 5% threshold."),
        ("Slicers — Region, project status, year", "Apply to every visual through the model relationships "
                      "(dim_project.region / status, dim_date.year)."),
        ("Build in Power BI", "Import outputs/results/mohammed_faisal/02_sql_and_viz/*.csv or the DuckDB tables; relationships: "
                      "fact_transactions -> dim_project / dim_vendor / dim_date; measures: Budget, Actual, Over-budget %."),
    ]
    y = 0.88
    for h, body in notes:
        f2.text(0.05, y, h, fontsize=12, fontweight="bold", color=INK)
        f2.text(0.05, y - 0.028, body, fontsize=10, color=MUTED, wrap=True)
        y -= 0.115
    pdf.savefig(f2)
print("Wrote", OUT)
