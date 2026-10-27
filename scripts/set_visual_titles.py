"""Give every report visual an explicit English title, hide the auto-generated subtitle and table totals.

Power BI writes default titles/subtitles in the UI language (e.g. "(by month_start)" in Chinese),
so explicit titles keep the report language-independent. Run with Power BI Desktop closed.
"""
import glob
import json
from pathlib import Path

PAGES = Path(__file__).resolve().parents[1] / "powerbi" / "retail_cost_to_serve.Report" / "definition" / "pages"

# (visual type, category field, first value field) -> title
TITLES = {
    ("lineChart", "dim_date.month_start", "fact_order_item.Revenue"): "Monthly revenue",
    ("barChart", "dim_product.category_name", "fact_order_item.Revenue"): "Top 10 categories by revenue",
    ("lineClusteredColumnComboChart", "dim_date.month_start", "fact_order_item.Revenue"): "Revenue vs budget by month",
    ("columnChart", "dim_date.month_start", "fact_budget.Variance %"): "Budget variance % by month",
    ("barChart", "dim_state.region", "fact_order_item.Freight %"): "Freight as % of revenue, by region",
    ("columnChart", "fact_order_item.delivery_status", "fact_order_item.Avg Review"): "Average review score: on time vs late",
    ("barChart", "dim_state.state_name", "fact_order_item.Freight %"): "Freight as % of revenue, by state",
    ("tableEx", None, "dim_state.region"): "Cost to serve by region",
    ("tableEx", None, "dq_results.layer"): "Data-quality checks (latest pipeline run)",
}


def lit(value):
    return {"expr": {"Literal": {"Value": value}}}


def main():
    for f in glob.glob(str(PAGES / "*" / "visuals" / "*" / "visual.json")):
        v = json.load(open(f, encoding="utf-8"))
        vis = v["visual"]
        qs = vis.get("query", {}).get("queryState", {})
        cat = (qs.get("Category", {}).get("projections") or [{}])[0].get("queryRef")
        first = None
        for role in ("Y", "Values"):
            if qs.get(role, {}).get("projections"):
                first = qs[role]["projections"][0]["queryRef"]
                break
        title = TITLES.get((vis["visualType"], cat, first))
        if not title:
            continue
        objs = vis.setdefault("visualContainerObjects", {})
        objs["title"] = [{"properties": {"show": lit("true"), "text": lit("'" + title + "'")}}]
        objs["subTitle"] = [{"properties": {"show": lit("false")}}]
        if vis["visualType"] == "tableEx":
            # the totals row label is rendered in the UI language ("Total" becomes Chinese); not needed here
            vis.setdefault("objects", {})["total"] = [{"properties": {"totals": lit("false")}}]
        json.dump(v, open(f, "w", encoding="utf-8"), indent=2)
        print(f"{vis['visualType']:<32} {title}")


if __name__ == "__main__":
    main()
