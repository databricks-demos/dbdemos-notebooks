#!/usr/bin/env python3
"""Generate the Patient Genomics / OncoTarget-1 RWE Lakeview dashboard.
Page 1 "Real-world evidence": KPIs, survival treated vs standard of care, survival by cancer site, UMAP molecular map.
Page 2 "Who benefits most": breast responder-subtype deep-dive, UMAP responder cluster, responder demographics, patient table.
Schema is the _v2 build schema; retargeted to default at port time.
"""
import json

S = "main.dbdemos_aibi_hls_genomics_v2"

# palette — keep the genomics demo's spirit (coral / teal / green) as a coherent theme
TEAL   = "#077A9D"   # OncoTarget-1 / primary (the good drug)
CORAL  = "#FCA4A1"   # standard of care / comparison (was female-coral)
GREEN  = "#00A972"   # responder / good
AMBER  = "#FB8D3D"   # accent / highlight
RED    = "#E92828"   # alert
BLUE   = "#356AFF"
GREY   = "#9AA7B4"
VIOLET = "#9B6A9E"
VIZ = [TEAL, CORAL, GREEN, AMBER, BLUE, VIOLET, "#5BC2D6", "#9CC6A6"]

def q(name, disp, lines): return {"name": name, "displayName": disp, "queryLines": lines}

datasets = [
    {"name": "ds_metrics", "displayName": "Genomics metrics", "asset_name": f"{S}.genomics_metrics"},
    q("ds_kpi_treated", "Survival treated", [f"SELECT survival_pct FROM {S}.arm_summary WHERE treatment_arm='OncoTarget-1'"]),
    q("ds_kpi_soc", "Survival SoC", [f"SELECT survival_pct FROM {S}.arm_summary WHERE treatment_arm='Standard of care'"]),
    q("ds_kpi_patients", "Patients", [f"SELECT count(*) AS patients FROM {S}.patient_cohort"]),
    q("ds_kpi_lift", "Responder lift", [
        f"SELECT round(max(CASE WHEN treatment_arm='OncoTarget-1' THEN survival_pct END)\n",
        f"  - max(CASE WHEN treatment_arm='Standard of care' THEN survival_pct END),1) AS lift\n",
        f"FROM {S}.breast_deepdive WHERE subgroup='Responder subtype'"]),
    q("ds_arm", "Survival by arm", [f"SELECT treatment_arm, survival_pct, patients FROM {S}.arm_summary ORDER BY treatment_arm"]),
    q("ds_curve", "Survival curve (overall)", [
        f"SELECT month, treatment_arm, pct_surviving FROM {S}.survival_curve ORDER BY month, treatment_arm"]),
    q("ds_curve_resp", "Survival curve (responder subtype)", [
        f"SELECT month, treatment_arm, pct_surviving FROM {S}.survival_curve_responder ORDER BY month, treatment_arm"]),
    q("ds_spark_treated", "Treated survival trend (sparkline)", [
        f"SELECT month, pct_surviving FROM {S}.survival_curve WHERE treatment_arm='OncoTarget-1' ORDER BY month"]),
    q("ds_spark_soc", "SoC survival trend (sparkline)", [
        f"SELECT month, pct_surviving FROM {S}.survival_curve WHERE treatment_arm='Standard of care' ORDER BY month"]),
    q("ds_site", "Survival by cancer site x arm", [
        f"SELECT tissue, treatment_arm, survival_pct, patients FROM {S}.survival_by_arm\n",
        f"WHERE patients > 80 ORDER BY tissue, treatment_arm"]),
    q("ds_lift", "Survival lift by cancer site", [
        f"SELECT tissue, survival_lift, patients,\n",
        f"  CASE WHEN survival_lift >= 3 THEN 'Benefits' WHEN survival_lift <= -3 THEN 'Not indicated' ELSE 'No effect' END AS verdict\n",
        f"FROM {S}.lift_by_site ORDER BY survival_lift DESC"]),
    q("ds_umap", "UMAP molecular map", [
        f"SELECT c1, c2, tissue,\n",
        f"  CASE WHEN responder_subtype THEN 'Responder subtype' ELSE tissue END AS cluster,\n",
        f"  treatment_arm, survived_24mo\n",
        f"FROM {S}.patient_cohort\n",
        f"WHERE c1 IS NOT NULL AND tissue IN ('Breast, NOS','Kidney, NOS','Upper lobe, lung','Thyroid gland','Prostate gland','Liver','Ovary','Skin, NOS')"]),
    q("ds_breast", "Breast deep-dive", [f"SELECT subgroup, treatment_arm, survival_pct, patients FROM {S}.breast_deepdive ORDER BY subgroup, treatment_arm"]),
    q("ds_umap_breast", "UMAP breast responders", [
        f"SELECT c1, c2, CASE WHEN responder_subtype THEN 'Responder subtype' ELSE 'Other breast' END AS subgroup, treatment_arm, survived_24mo\n",
        f"FROM {S}.patient_cohort WHERE tissue='Breast, NOS' AND c1 IS NOT NULL"]),
    q("ds_responder_demo", "Responder demographics", [
        f"SELECT race, count(*) patients, round(avg(age)) avg_age\n",
        f"FROM {S}.patient_cohort WHERE responder_subtype GROUP BY 1 HAVING patients>10 ORDER BY patients DESC"]),
    q("ds_patients", "Patient table", [
        f"SELECT case_id, primary_diagnosis, gender, age, treatment_arm,\n",
        f"  CASE WHEN survived_24mo THEN 'Survived' ELSE 'Deceased' END AS outcome_24mo, months_survived\n",
        f"FROM {S}.patient_cohort WHERE tissue='Breast, NOS' AND responder_subtype ORDER BY treatment_arm, months_survived DESC LIMIT 200"]),
]

def text(name, lines, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [lines]}}, "position": {"x": x, "y": y, "width": w, "height": h}}

def counter(name, ds, field, title, disp, x, y, fmt=None, tmpl=None, color=None, w=3, h=3):
    enc = {"value": {"fieldName": field, "displayName": disp}}
    if fmt: enc["value"]["format"] = fmt
    if tmpl: enc["value"]["formatTemplate"] = tmpl
    if color: enc["value"]["color"] = {"default": color}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds,
        "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": enc, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

NUM1 = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}}
NUM0 = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 0}}

ARM_COLORS = [{"value": "OncoTarget-1", "color": TEAL}, {"value": "Standard of care", "color": CORAL}]

def counter_spark(name, ds, field, period, title, disp, x, y, fmt=None, tmpl=None, color=None, w=3, h=3):
    enc = {"value": {"fieldName": field, "displayName": disp}, "period": {"fieldName": period}}
    if fmt: enc["value"]["format"] = fmt
    if tmpl: enc["value"]["formatTemplate"] = tmpl
    if color: enc["value"]["color"] = {"default": color}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds,
        "fields": [{"name": field, "expression": f"`{field}`"}, {"name": period, "expression": f"`{period}`"}], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": enc, "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def km_curve(name, ds, title, x, y, w, h):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds,
        "fields": [{"name": "month", "expression": "`month`"}, {"name": "pct_surviving", "expression": "`pct_surviving`"}, {"name": "treatment_arm", "expression": "`treatment_arm`"}],
        "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "line", "encodings": {
            "x": {"fieldName": "month", "scale": {"type": "quantitative"}, "displayName": "Months since treatment start"},
            "y": {"fieldName": "pct_surviving", "scale": {"type": "quantitative"}, "displayName": "% surviving"},
            "color": {"fieldName": "treatment_arm", "scale": {"type": "categorical", "mappings": ARM_COLORS}, "displayName": "Treatment"}},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

# ===================== PAGE 1 =====================
p1 = []
p1.append(text("title",
    "# OncoTarget-1 — it works… but not everywhere. *Where* should we use it?\n\nWe rolled out a new targeted therapy across a real 10,000-patient oncology cohort. Overall, treated patients survive better — but the benefit is **wildly uneven**: a near-miracle in some cancers, nothing in others, and in a couple it may even do harm. The chart below ranks every cancer by survival lift. The obvious follow-up: *what separates the responders from the rest?* (Answer on page 2 — and it's molecular.)",
    0, 0, 12, 2))
# KPIs — survival counters carry a sparkline of their survival curve over 24 months
p1.append(counter("kpi_patients", "ds_kpi_patients", "patients", "Patients in cohort", "Patients", 0, 2, NUM0, None, None))
p1.append(counter_spark("kpi_treated", "ds_spark_treated", "pct_surviving", "month", "OncoTarget-1 survival (24mo)", "Treated", 3, 2, NUM1, "{{ @formatted }}%", TEAL))
p1.append(counter_spark("kpi_soc", "ds_spark_soc", "pct_surviving", "month", "Standard of care survival (24mo)", "Standard", 6, 2, NUM1, "{{ @formatted }}%", CORAL))
p1.append(counter("kpi_lift", "ds_kpi_lift", "lift", "Best-responder survival lift", "Responder lift", 9, 2, NUM1, "+{{ @formatted }} pts", GREEN))

# HERO: the diverging "survival lift by cancer" bar — the puzzle (green helps, red harms)
p1.append({"widget": {"name": "lift_bar", "queries": [{"name": "main_query", "query": {"datasetName": "ds_lift",
    "fields": [{"name": "tissue", "expression": "`tissue`"}, {"name": "survival_lift", "expression": "`survival_lift`"}, {"name": "verdict", "expression": "`verdict`"}],
    "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "bar", "encodings": {
        "y": {"fieldName": "tissue", "scale": {"type": "categorical"}, "displayName": "Cancer site"},
        "x": {"fieldName": "survival_lift", "scale": {"type": "quantitative"}, "displayName": "Survival lift vs standard of care (pts)"},
        "color": {"fieldName": "verdict", "scale": {"type": "categorical", "mappings": [
            {"value": "Benefits", "color": GREEN}, {"value": "No effect", "color": GREY}, {"value": "Not indicated", "color": RED}]}, "displayName": ""}},
        "frame": {"showTitle": True, "title": "Survival lift by cancer site — where OncoTarget-1 helps (green), does nothing (grey), or harms (red)"}}},
    "position": {"x": 0, "y": 5, "width": 7, "height": 7}})

# UMAP molecular map — the "why" hint
p1.append({"widget": {"name": "umap_map", "queries": [{"name": "main_query", "query": {"datasetName": "ds_umap",
    "fields": [{"name": "c1", "expression": "`c1`"}, {"name": "c2", "expression": "`c2`"}, {"name": "cluster", "expression": "`cluster`"}, {"name": "tissue", "expression": "`tissue`"}],
    "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "scatter", "encodings": {
        "x": {"fieldName": "c1", "scale": {"type": "quantitative"}, "displayName": "UMAP-1"},
        "y": {"fieldName": "c2", "scale": {"type": "quantitative"}, "displayName": "UMAP-2"},
        "color": {"fieldName": "cluster", "scale": {"type": "categorical"}, "displayName": "Molecular cluster"},
        "extra": [{"fieldName": "tissue", "displayName": "Cancer site"}, {"fieldName": "cluster", "displayName": "Cluster"}]},
        "frame": {"showTitle": True, "title": "Gene-expression map (UMAP) — the responders share a molecular signature, not an organ"}}},
    "position": {"x": 7, "y": 5, "width": 5, "height": 7}})

# supporting: overall KM curve
p1.append(km_curve("km_overall", "ds_curve", "Overall survival over time — treated vs standard of care (the average hides the unevenness above)", 0, 12, 12, 6))

# ===================== PAGE 2 =====================
p2 = []
p2.append(text("d_title",
    "# Who benefits most — the breast responder subtype\n\nDeep-dive into breast cancer. Patients split into a **responder molecular subtype** (a distinct gene-expression cluster) and the rest. In the responder subtype, OncoTarget-1 lifts 24-month survival from ~89% to ~99% — far more than in other breast patients. This is the kind of precision-oncology signal that turns a good drug into the *right* drug for the *right* patient.",
    0, 0, 12, 2))

p2.append(text("d_step1", "## 1. In the responder subtype, the survival curves split wide open", 0, 2, 12, 1))
# the dramatic KM curve for the responder subtype (treated stays near 99%, SoC drops to ~89%)
p2.append(km_curve("km_responder", "ds_curve_resp", "Responder-subtype survival over time — OncoTarget-1 holds near 99% while standard of care falls", 0, 3, 7, 6))

# UMAP zoom on breast
p2.append({"widget": {"name": "umap_breast", "queries": [{"name": "main_query", "query": {"datasetName": "ds_umap_breast",
    "fields": [{"name": "c1", "expression": "`c1`"}, {"name": "c2", "expression": "`c2`"}, {"name": "subgroup", "expression": "`subgroup`"}],
    "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "scatter", "encodings": {
        "x": {"fieldName": "c1", "scale": {"type": "quantitative"}, "displayName": "UMAP-1"},
        "y": {"fieldName": "c2", "scale": {"type": "quantitative"}, "displayName": "UMAP-2"},
        "color": {"fieldName": "subgroup", "scale": {"type": "categorical", "mappings": [
            {"value": "Responder subtype", "color": GREEN}, {"value": "Other breast", "color": GREY}]}, "displayName": "Subgroup"}},
        "frame": {"showTitle": True, "title": "Breast cancer gene-expression — the responder subtype is a distinct molecular cluster"}}},
    "position": {"x": 7, "y": 3, "width": 5, "height": 6}})

p2.append(text("d_step2", "## 2. Responder subtype vs the rest of breast — and who the responders are", 0, 9, 12, 1))
# compact responder-vs-other bar comparison
p2.append({"widget": {"name": "breast_bar", "queries": [{"name": "main_query", "query": {"datasetName": "ds_breast",
    "fields": [{"name": "subgroup", "expression": "`subgroup`"}, {"name": "survival_pct", "expression": "`survival_pct`"}, {"name": "treatment_arm", "expression": "`treatment_arm`"}],
    "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "bar", "encodings": {
        "y": {"fieldName": "subgroup", "scale": {"type": "categorical"}, "displayName": "Breast subgroup"},
        "x": {"fieldName": "survival_pct", "scale": {"type": "quantitative"}, "displayName": "24-month survival %"},
        "color": {"fieldName": "treatment_arm", "scale": {"type": "categorical", "mappings": ARM_COLORS}, "displayName": "Treatment"},
        "mark": {"layout": "group"}},
        "frame": {"showTitle": True, "title": "Breast survival — responder subtype vs other (treated vs standard of care)"}}},
    "position": {"x": 0, "y": 10, "width": 5, "height": 6}})
p2.append({"widget": {"name": "responder_demo", "queries": [{"name": "main_query", "query": {"datasetName": "ds_responder_demo",
    "fields": [{"name": "race", "expression": "`race`"}, {"name": "patients", "expression": "`patients`"}],
    "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "pie", "encodings": {
        "angle": {"fieldName": "patients", "scale": {"type": "quantitative"}, "displayName": "Patients"},
        "color": {"fieldName": "race", "scale": {"type": "categorical"}, "displayName": "Race"},
        "label": {"show": True}},
        "frame": {"showTitle": True, "title": "Responder subtype — patient mix"}}},
    "position": {"x": 5, "y": 10, "width": 3, "height": 6}})
p2.append({"widget": {"name": "patient_table", "queries": [{"name": "main_query", "query": {"datasetName": "ds_patients",
    "fields": [{"name": c, "expression": f"`{c}`"} for c in ["case_id", "primary_diagnosis", "gender", "age", "treatment_arm", "outcome_24mo", "months_survived"]],
    "disaggregated": True}}],
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": "case_id", "displayName": "Patient"},
        {"fieldName": "primary_diagnosis", "displayName": "Diagnosis"},
        {"fieldName": "gender", "displayName": "Gender"},
        {"fieldName": "age", "displayName": "Age"},
        {"fieldName": "treatment_arm", "displayName": "Treatment"},
        {"fieldName": "outcome_24mo", "displayName": "24mo outcome"},
        {"fieldName": "months_survived", "displayName": "Months"}]},
        "frame": {"showTitle": True, "title": "Responder-subtype patients"}}},
    "position": {"x": 8, "y": 10, "width": 4, "height": 6}})

# ===================== FILTERS =====================
def filt(name, qn, field, x):
    return {"widget": {"name": name, "queries": [{"name": qn, "query": {"datasetName": "ds_metrics",
        "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "filter-multi-select", "encodings": {"fields": [{"fieldName": field, "displayName": field, "queryName": qn}]},
        "frame": {"showTitle": True, "title": field}}}, "position": {"x": x, "y": 0, "width": 3, "height": 1}}
filters = [filt("f_arm", "f_a", "Treatment Arm", 0), filt("f_site", "f_s", "Cancer Site", 3), filt("f_gender", "f_g", "Gender", 6), filt("f_race", "f_r", "Race", 9)]

dash = {
    "datasets": datasets,
    "pages": [
        {"name": "evidence", "displayName": "Real-world evidence", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p1},
        {"name": "responders", "displayName": "Who benefits most", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p2},
        {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1", "layout": filters},
    ],
    "uiSettings": {
        "theme": {
            "canvasBackgroundColor": {"light": "#FCFCFC", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#077A9D", "dark": "#5BC2D6"},
            "visualizationColors": VIZ, "widgetHeaderAlignment": "LEFT",
        },
        "genieSpace": {"isEnabled": True, "overrideId": "", "enablementMode": "ENABLED"},
    },
}

for p in dash["pages"]:
    if p["pageType"] != "PAGE_TYPE_CANVAS": continue
    cells = {}
    for w in p["layout"]:
        po = w["position"]
        for yy in range(po["y"], po["y"]+po["height"]):
            for xx in range(po["x"], po["x"]+po["width"]):
                assert (yy, xx) not in cells, f"OVERLAP {p['name']} {(yy,xx)} {cells.get((yy,xx))} vs {w['widget']['name']}"
                cells[(yy, xx)] = w["widget"]["name"]

out = "aibi/aibi-patient-genomics/_build_v2/dashboard.json"
json.dump(dash, open(out, "w"), indent=2)
print("OK —", out, "| pages:", [p["displayName"] for p in dash["pages"]], "| datasets:", len(datasets))
