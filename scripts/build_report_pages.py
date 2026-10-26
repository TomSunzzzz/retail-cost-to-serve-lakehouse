"""Generate Power BI report pages 2-4 as PBIR files (Budget vs Actual, Cost to Serve, Data Quality).

Page 1 (Sales Overview) was built by hand in Power BI Desktop; these pages follow the same
PBIR format. Run with Power BI Desktop closed, then reopen powerbi/retail_cost_to_serve.pbip.
"""
import json
import secrets
from pathlib import Path

DEF = Path(__file__).resolve().parents[1] / "powerbi" / "retail_cost_to_serve.Report" / "definition"
PAGES = DEF / "pages"
VISUAL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.14.0/schema.json"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"


def uid():
    return secrets.token_hex(10)


def measure(entity, prop):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def column(entity, prop):
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def proj(field, active=False):
    kind = "Measure" if "Measure" in field else "Column"
    entity = field[kind]["Expression"]["SourceRef"]["Entity"]
    prop = field[kind]["Property"]
    p = {"field": field, "queryRef": f"{entity}.{prop}", "nativeQueryRef": prop}
    if active:
        p["active"] = True
    return p


def visual(page_dir, vtype, x, y, w, h, z, query_state, sort=None):
    name = uid()
    v = {
        "$schema": VISUAL_SCHEMA,
        "name": name,
        "position": {"x": x, "y": y, "z": z, "height": h, "width": w, "tabOrder": z},
        "visual": {"visualType": vtype, "query": {"queryState": query_state}, "drillFilterOtherVisuals": True},
    }
    if sort:
        field, direction = sort
        v["visual"]["query"]["sortDefinition"] = {"sort": [{"field": field, "direction": direction}], "isDefaultSort": True}
    out = page_dir / "visuals" / name
    out.mkdir(parents=True)
    (out / "visual.json").write_text(json.dumps(v, indent=2), encoding="utf-8")


def card(page_dir, field, x, z):
    visual(page_dir, "cardVisual", x, 0, 280, 280, z, {"Data": {"projections": [proj(field)]}})


REPORTING_PERIOD_FILTER = {
    "field": column("dim_date", "in_reporting_period"),
    "type": "Categorical",
    "filter": {
        "Version": 2,
        "From": [{"Name": "d", "Entity": "dim_date", "Type": 0}],
        "Where": [{"Condition": {"In": {
            "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "in_reporting_period"}}],
            "Values": [[{"Literal": {"Value": "true"}}]],
        }}}],
    },
    "howCreated": "User",
}


def page(name, display_name, period_filter=True):
    page_dir = PAGES / name
    page_dir.mkdir(exist_ok=True)
    p = {"$schema": PAGE_SCHEMA, "name": name, "displayName": display_name,
         "displayOption": "FitToPage", "height": 1080, "width": 1920}
    if period_filter:
        p["filterConfig"] = {"filters": [dict(REPORTING_PERIOD_FILTER, name=uid())]}
    (page_dir / "page.json").write_text(json.dumps(p, indent=2), encoding="utf-8")
    return page_dir


def main():
    meta = json.loads((PAGES / "pages.json").read_text(encoding="utf-8"))
    order = meta["pageOrder"]
    by_name = {json.loads((PAGES / n / "page.json").read_text(encoding="utf-8"))["displayName"]: n for n in order}

    F, B = "fact_order_item", "fact_budget"
    region = column("dim_state", "region")
    month = column("dim_date", "month_start")

    # ---- page 2: Budget vs Actual (reuse the empty page Tom created) ----
    p2 = page(by_name.get("Budget vs Actual", uid()), "Budget vs Actual")
    for i, f in enumerate([measure(F, "Revenue"), measure(B, "Budget"), measure(B, "Variance"), measure(B, "Variance %")]):
        card(p2, f, 10 + i * 290, i * 1000)
    visual(p2, "lineClusteredColumnComboChart", 10, 290, 1250, 780, 5000, {
        "Category": {"projections": [proj(month, active=True)]},
        "Y": {"projections": [proj(measure(F, "Revenue"))]},
        "Y2": {"projections": [proj(measure(B, "Budget"))]},
    }, sort=(month, "Ascending"))
    visual(p2, "barChart", 1270, 290, 640, 780, 6000, {
        "Category": {"projections": [proj(region, active=True)]},
        "Y": {"projections": [proj(measure(B, "Variance %"))]},
    }, sort=(measure(B, "Variance %"), "Ascending"))

    # ---- page 3: Cost to Serve ----
    p3_name = by_name.get("Cost to Serve", uid())
    p3 = page(p3_name, "Cost to Serve")
    for i, f in enumerate([measure(F, "Freight %"), measure(F, "Avg Distance km"),
                           measure(F, "Avg Delivery Days"), measure(F, "Late %"), measure(F, "Avg Review")]):
        card(p3, f, 10 + i * 290, i * 1000)
    visual(p3, "barChart", 10, 290, 620, 400, 6000, {
        "Category": {"projections": [proj(region, active=True)]},
        "Y": {"projections": [proj(measure(F, "Freight %"))]},
    }, sort=(measure(F, "Freight %"), "Descending"))
    visual(p3, "columnChart", 640, 290, 620, 400, 7000, {
        "Category": {"projections": [proj(column(F, "is_late"), active=True)]},
        "Y": {"projections": [proj(measure(F, "Avg Review"))]},
    })
    visual(p3, "barChart", 1270, 290, 640, 780, 8000, {
        "Category": {"projections": [proj(column("dim_state", "state_name"), active=True)]},
        "Y": {"projections": [proj(measure(F, "Freight %"))]},
    }, sort=(measure(F, "Freight %"), "Descending"))
    visual(p3, "tableEx", 10, 700, 1250, 370, 9000, {
        "Values": {"projections": [proj(region), proj(measure(F, "Revenue")), proj(measure(F, "Freight %")),
                                   proj(measure(F, "Avg Distance km")), proj(measure(F, "Avg Delivery Days")),
                                   proj(measure(F, "Late %")), proj(measure(F, "Avg Review"))]},
    }, sort=(measure(F, "Freight %"), "Descending"))

    # ---- page 4: Data Quality ----
    p4_name = by_name.get("Data Quality", uid())
    p4 = page(p4_name, "Data Quality", period_filter=False)
    D = "dq_results"
    visual(p4, "tableEx", 10, 10, 1900, 1060, 1000, {
        "Values": {"projections": [proj(column(D, c)) for c in
                                   ("layer", "table_name", "rule", "severity", "status", "failed_rows", "total_rows", "note")]},
    })

    for n in (p2.name, p3.name, p4.name):
        if n not in order:
            order.append(n)
    meta["activePageName"] = order[0]
    (PAGES / "pages.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print("pages:", [json.loads((PAGES / n / "page.json").read_text(encoding="utf-8"))["displayName"] for n in order])


if __name__ == "__main__":
    main()
