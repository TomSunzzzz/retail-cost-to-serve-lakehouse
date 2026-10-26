"""Build review/lakehouse_review.xlsx: a workbook a person can open in Excel to audit the pipeline.

Runs on the host (needs pandas, pyarrow, openpyxl), after:
  docker compose run --rm spark python -m src.run_pipeline
  docker compose run --rm spark python -m scripts.export_review_extracts
Then:
  python scripts/build_review_workbook.py

Summary sheets use Excel formulas over the full fact tables, next to the value the pipeline
computed, with a Match column, so a reviewer can verify the numbers independently.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parents[1]
EXPORT = ROOT / "lakehouse" / "export"
REVIEW = ROOT / "lakehouse" / "review"
OUT = ROOT / "review" / "lakehouse_review.xlsx"

PERIOD_START, PERIOD_END = date(2017, 1, 1), date(2018, 8, 31)
FONT = "Arial"
HEADER_FILL = PatternFill("solid", fgColor="1F3864")
INPUT_FONT = Font(name=FONT, color="0000FF")
BOLD = Font(name=FONT, bold=True)
MONEY = 'R$ #,##0;-R$ #,##0;"-"'
MONEY2 = 'R$ #,##0.00;-R$ #,##0.00;"-"'
PCT = '0.0%;-0.0%;"-"'


def read(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    for col in df.columns:
        sample = df[col].dropna().head(1)
        if len(sample) and isinstance(sample.iloc[0], Decimal):
            df[col] = df[col].astype(float)
    return df


def header(ws, row, values):
    for i, v in enumerate(values, start=1):
        c = ws.cell(row=row, column=i, value=v)
        c.font = Font(name=FONT, bold=True, color="FFFFFF")
        c.fill = HEADER_FILL
        c.alignment = Alignment(wrap_text=True, vertical="center")


def data_sheet(wb, name, df, note, widths=None, formats=None):
    """One table per sheet: a note on row 1, headers on row 3, filter and frozen header."""
    ws = wb.create_sheet(name)
    ws["A1"] = note
    ws["A1"].font = Font(name=FONT, italic=True, color="595959")
    header(ws, 3, list(df.columns))
    for r in df.itertuples(index=False):
        ws.append([None if (isinstance(v, float) and pd.isna(v)) or v is pd.NA else v for v in r])
    ws.freeze_panes = "A4"
    ws.auto_filter.ref = f"A3:{get_column_letter(df.shape[1])}{df.shape[0] + 3}"
    for i, col in enumerate(df.columns, start=1):
        letter = get_column_letter(i)
        ws.column_dimensions[letter].width = (widths or {}).get(col, max(10, min(34, len(col) + 4)))
        if formats and col in formats:
            for cell in ws[letter][3:]:
                cell.number_format = formats[col]
    return ws


def col_of(df, name):
    return get_column_letter(list(df.columns).index(name) + 1)


def main():
    wb = Workbook()
    wb._named_styles["Normal"].font = Font(name=FONT, size=10)

    fact = read(EXPORT / "fact_order_item.parquet")
    budget = read(EXPORT / "fact_budget.parquet")
    states = read(EXPORT / "dim_state.parquet")
    dq = read(EXPORT / "dq_results.parquet")
    counts = read(REVIEW / "row_counts.parquet")
    sellers_clean = read(REVIEW / "seller_city_cleaning.parquet")
    categories = read(REVIEW / "category_mapping.parquet")

    # region on the fact sheet so region summaries can be checked with plain SUMIFS
    fact = fact.merge(states[["state", "region"]].rename(columns={"state": "customer_state", "region": "customer_region"}),
                      on="customer_state", how="left")
    fact["order_date"] = pd.to_datetime(fact["order_date"])
    budget["month_start"] = pd.to_datetime(budget["month_start"])
    in_period = fact[fact.is_revenue & fact.order_date.between(pd.Timestamp(PERIOD_START), pd.Timestamp(PERIOD_END))]

    # ---------------- README ----------------
    ws = wb.active
    ws.title = "README"
    lines = [
        ("Lakehouse review workbook", BOLD),
        ("Retail Sales & Cost-to-Serve Lakehouse - Olist public e-commerce data", None),
        (f"Pipeline run: {dq['run_id'].iloc[0]}", None),
        ("", None),
        ("Sheets", BOLD),
        ("Check_Budget      Actual vs budget by month. Excel formulas over the fact sheets, beside the pipeline's own figure.", None),
        ("Check_Region      Cost-to-serve by customer region, same approach.", None),
        ("Row_Counts        Rows in every table at every layer (Bronze = raw, Silver = cleaned, Gold = star schema).", None),
        ("DQ_Results        All 46 data-quality checks from this run. FAIL stops the pipeline; WARN is recorded.", None),
        ("Clean_SellerCity  Every seller city before and after cleaning. Filter 'changed' = TRUE to audit the rule.", None),
        ("Clean_Category    Portuguese product categories and the English name used in reports.", None),
        ("fact_order_item   Gold fact table, one row per item sold (112,650 rows). Filter freely.", None),
        ("fact_budget       Gold budget fact, one row per month and state. SYNTHETIC - see note on the sheet.", None),
        ("dim_*             Gold dimension tables.", None),
        ("", None),
        ("Definitions", BOLD),
        ("Revenue           Item price of orders with status 'delivered'. Freight is a cost, not revenue.", None),
        (f"Reporting period  {PERIOD_START} to {PERIOD_END}. Earlier and later months hold only a handful of orders.", None),
        ("Currency          Brazilian real (R$).", None),
        ("", None),
        ("How to review", BOLD),
        ("1. Check_Budget and Check_Region: every Match cell should read OK. Excel recomputes from the raw rows.", None),
        ("2. Row_Counts: Bronze equals the source CSV; Silver geolocation is one row per zip code; Gold fact equals Silver order_items.", None),
        ("3. DQ_Results: no FAIL rows. Read the note on each WARN.", None),
        ("4. Clean_SellerCity: filter changed = TRUE and confirm each 'after' value is the right city.", None),
    ]
    for i, (text, font) in enumerate(lines, start=1):
        ws.cell(row=i, column=1, value=text).font = font or Font(name=FONT)
    ws.column_dimensions["A"].width = 120

    # ---------------- data sheets (written first so summaries can reference them) ----------------
    fact_cols = list(fact.columns)
    data_sheet(wb, "fact_order_item", fact, "Gold fact table: one row per item sold. Revenue rows: is_revenue = TRUE.",
               widths={"order_id": 34, "customer_id": 34, "seller_id": 34, "product_id": 34, "order_date": 12},
               formats={"price": MONEY2, "freight_value": MONEY2, "item_total": MONEY2, "order_date": "yyyy-mm-dd"})
    data_sheet(wb, "fact_budget", budget,
               "SYNTHETIC budget (Olist publishes none): 3-month smoothed actual revenue x 1.05 target, "
               "seeded +/-6% variation, split by each state's revenue share.",
               formats={"budget_revenue": MONEY2, "month_start": "yyyy-mm"})

    # ---------------- Check_Budget ----------------
    ws = wb.create_sheet("Check_Budget", 1)
    F, B = "fact_order_item", "fact_budget"
    rev_rng, flag_rng, date_rng = (f"{F}!${col_of(fact, c)}:${col_of(fact, c)}" for c in ("price", "is_revenue", "order_date"))
    bud_rng, bmon_rng = (f"{B}!${col_of(budget, c)}:${col_of(budget, c)}" for c in ("budget_revenue", "month_start"))
    ws["A1"] = "Actual vs budget by month (revenue = delivered item price, R$)"
    ws["A1"].font = Font(name=FONT, bold=True, size=12)
    ws["A2"] = "Columns B-E are Excel formulas over the full fact sheets. Column F is the pipeline's figure. G compares them."
    header(ws, 4, ["Month", "Actual (formula)", "Budget (formula)", "Variance", "Variance %",
                   "Actual (pipeline)", "Match"])
    pipeline_monthly = in_period.groupby(in_period.order_date.dt.to_period("M"))["price"].sum()
    months = pd.period_range("2017-01", "2018-08", freq="M")
    for i, m in enumerate(months):
        r = 5 + i
        ws.cell(row=r, column=1, value=m.to_timestamp().date()).number_format = "yyyy-mm"
        ws.cell(row=r, column=2, value=f'=SUMIFS({rev_rng},{flag_rng},TRUE,{date_rng},">="&A{r},{date_rng},"<"&EDATE(A{r},1))')
        ws.cell(row=r, column=3, value=f"=SUMIFS({bud_rng},{bmon_rng},A{r})")
        ws.cell(row=r, column=4, value=f"=B{r}-C{r}")
        ws.cell(row=r, column=5, value=f'=IF(C{r}=0,"",B{r}/C{r}-1)')
        ws.cell(row=r, column=6, value=round(float(pipeline_monthly.get(m, 0.0)), 2)).font = INPUT_FONT
        ws.cell(row=r, column=7, value=f'=IF(ROUND(B{r}-F{r},2)=0,"OK","CHECK")')
    last = 4 + len(months)
    t = last + 1
    ws.cell(row=t, column=1, value="Total").font = BOLD
    for c in "BCDF":
        ws[f"{c}{t}"] = f"=SUM({c}5:{c}{last})"
        ws[f"{c}{t}"].font = BOLD
    ws[f"E{t}"] = f'=IF(C{t}=0,"",B{t}/C{t}-1)'
    ws[f"G{t}"] = f'=IF(ROUND(B{t}-F{t},2)=0,"OK","CHECK")'
    for r in range(5, t + 1):
        for c in "BCDF":
            ws[f"{c}{r}"].number_format = MONEY
        ws[f"E{r}"].number_format = PCT
    ws["F4"].comment = Comment("Hardcoded: monthly revenue computed by the pipeline export (pandas on fact_order_item.parquet).", "pipeline")
    for c, w in zip("ABCDEFG", (10, 18, 18, 16, 12, 18, 9)):
        ws.column_dimensions[c].width = w
    ws.freeze_panes = "A5"

    # ---------------- Check_Region ----------------
    ws = wb.create_sheet("Check_Region", 2)
    reg_rng, frt_rng, dist_rng, days_rng, late_rng, rev_score_rng = (
        f"{F}!${col_of(fact, c)}:${col_of(fact, c)}"
        for c in ("customer_region", "freight_value", "distance_km", "delivery_days", "is_late", "review_score")
    )
    ws["A1"] = "Cost-to-serve by customer region (delivered orders, reporting period)"
    ws["A1"].font = Font(name=FONT, bold=True, size=12)
    ws["A2"], ws["B2"] = "Period start", PERIOD_START
    ws["A3"], ws["B3"] = "Period end", PERIOD_END
    for cell in ("B2", "B3"):
        ws[cell].number_format = "yyyy-mm-dd"
        ws[cell].font = INPUT_FONT
    header(ws, 5, ["Region", "Revenue", "Freight", "Freight % of revenue", "Avg distance (km)",
                   "Avg delivery (days)", "Late %", "Avg review (1-5)", "Freight % (pipeline)", "Match"])
    crit = f'{flag_rng},TRUE,{date_rng},">="&$B$2,{date_rng},"<="&$B$3'
    pipe = in_period.groupby("customer_region").agg(rev=("price", "sum"), frt=("freight_value", "sum"))
    pipe["pct"] = pipe.frt / pipe.rev
    for i, region in enumerate(pipe.sort_values("pct", ascending=False).index):
        r = 6 + i
        rc = f'{crit},{reg_rng},A{r}'
        ws.cell(row=r, column=1, value=region)
        ws.cell(row=r, column=2, value=f"=SUMIFS({rev_rng},{rc})").number_format = MONEY
        ws.cell(row=r, column=3, value=f"=SUMIFS({frt_rng},{rc})").number_format = MONEY
        ws.cell(row=r, column=4, value=f'=IF(B{r}=0,"",C{r}/B{r})').number_format = PCT
        ws.cell(row=r, column=5, value=f"=AVERAGEIFS({dist_rng},{rc})").number_format = "#,##0"
        ws.cell(row=r, column=6, value=f"=AVERAGEIFS({days_rng},{rc})").number_format = "0.0"
        ws.cell(row=r, column=7, value=f"=COUNTIFS({rc},{late_rng},TRUE)/(COUNTIFS({rc},{late_rng},TRUE)+COUNTIFS({rc},{late_rng},FALSE))").number_format = PCT
        ws.cell(row=r, column=8, value=f"=AVERAGEIFS({rev_score_rng},{rc})").number_format = "0.00"
        p = ws.cell(row=r, column=9, value=round(float(pipe.loc[region, "pct"]), 6))
        p.number_format, p.font = PCT, INPUT_FONT
        ws.cell(row=r, column=10, value=f'=IF(ABS(D{r}-I{r})<0.0005,"OK","CHECK")')
    ws["I5"].comment = Comment("Hardcoded: freight / revenue per region computed by the pipeline export (pandas).", "pipeline")
    for c, w in zip("ABCDEFGHIJ", (14, 16, 14, 12, 12, 12, 9, 11, 12, 8)):
        ws.column_dimensions[c].width = w

    # ---------------- audit sheets ----------------
    pivot = counts.pivot_table(index="table_name", columns="layer", values="row_count", aggfunc="sum")
    pivot = pivot.reindex(columns=["bronze", "silver", "gold"]).reset_index()
    data_sheet(wb, "Row_Counts", pivot, "Rows per table per layer. Gold tables have their own names (fact_*, dim_*).",
               formats={"bronze": "#,##0", "silver": "#,##0", "gold": "#,##0"})
    wb.move_sheet("Row_Counts", offset=-(len(wb.sheetnames) - 4))
    dq_view = dq[["layer", "table_name", "rule", "severity", "status", "failed_rows", "total_rows", "failed_pct", "note"]]
    data_sheet(wb, "DQ_Results", dq_view, "Data-quality checks for this run. FAIL = pipeline stops; WARN = known issue, handled.",
               widths={"rule": 40, "note": 60}, formats={"failed_rows": "#,##0", "total_rows": "#,##0"})
    wb.move_sheet("DQ_Results", offset=-(len(wb.sheetnames) - 5))
    data_sheet(wb, "Clean_SellerCity", sellers_clean,
               "Seller city as typed by sellers (before) and after normalisation. changed = TRUE where the rule altered it.",
               widths={"seller_id": 34, "city_before": 40, "city_after": 28})
    wb.move_sheet("Clean_SellerCity", offset=-(len(wb.sheetnames) - 6))
    data_sheet(wb, "Clean_Category", categories,
               "Product categories: original Portuguese, English used in reports, and number of products. Blank original = no category in source.",
               widths={"category_original": 46, "category_english": 40})
    wb.move_sheet("Clean_Category", offset=-(len(wb.sheetnames) - 7))

    for name, note in [
        ("dim_date", "One row per day."),
        ("dim_state", "27 Brazilian states and their regions."),
        ("dim_seller", "One row per seller, cleaned city, coordinates of the zip code."),
        ("dim_product", "One row per product, English category."),
        ("dim_customer", "One row per customer_id (customer_unique_id identifies the person)."),
    ]:
        df = read(EXPORT / f"{name}.parquet")
        for c in df.columns:
            if df[c].dtype == object and len(df[c].dropna()) and isinstance(df[c].dropna().iloc[0], date):
                df[c] = pd.to_datetime(df[c])
        data_sheet(wb, name, df, note, formats={"date": "yyyy-mm-dd", "month_start": "yyyy-mm"})

    OUT.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT)
    print(f"written {OUT} ({OUT.stat().st_size / 1e6:.1f} MB), sheets: {', '.join(wb.sheetnames)}")
    print(f"pipeline check: revenue in period R$ {in_period.price.sum():,.2f}, budget R$ {budget.budget_revenue.sum():,.2f}")


if __name__ == "__main__":
    main()
