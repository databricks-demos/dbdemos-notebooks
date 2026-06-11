#!/usr/bin/env python3
"""Reproducible medallion build for the patient-genomics / OncoTarget-1 RWE demo.

The 4 RAW tables (demographics, diagnoses, exposures, expression_profiles_umap) come from
../dbdemos-dataset/aibi/dbdemos_aibi_hls_genomics_v2/. Everything else — the synthetic
OncoTarget-1 treatment+outcome layer (patient_cohort), the survival rollups, and the
metric view — is DERIVED deterministically by the SQL in bundle_config.py's `sql_queries`
(hash(case_id) gives a stable pseudo-random arm + outcome, so no external generator is needed).

This script is the safekeeping copy: it reads the exact `sql_queries` out of bundle_config.py
and runs them against a build schema, so the gold layer can be rebuilt outside dbdemos.

Usage:
  python medallion_build.py [--schema dbdemos_aibi_hls_genomics_v2] [--catalog main] [--warehouse <id>]
Assumes the 4 raw tables already exist in the target schema (load them from the _v2 parquet first).
"""
import argparse, ast, json, os, subprocess, sys

def load_sql_queries(bundle_path):
    src = open(bundle_path).read()
    i = src.index("{", src.index("# COMMAND"))
    cfg = ast.literal_eval(src[i:src.rindex("}") + 1])
    return cfg["sql_queries"]

def run(sql, catalog, schema, warehouse):
    s = sql.replace("{{CATALOG}}", catalog).replace("{{SCHEMA}}", schema)
    r = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--json", json.dumps({"warehouse_id": warehouse, "statement": s})],
        capture_output=True, text=True,
        env={**os.environ, "DATABRICKS_CONFIG_PROFILE": os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")})
    d = json.loads(r.stdout)
    st = d["status"]["state"]
    if st != "SUCCEEDED":
        err = d["status"].get("error")
        print("  FAIL:", st, (err.get("message") if isinstance(err, dict) else err))
        return False
    return True

if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="main")
    ap.add_argument("--schema", default="dbdemos_aibi_hls_genomics_v2")
    ap.add_argument("--warehouse", default=os.environ.get("DATABRICKS_WAREHOUSE_ID", "9d8a677b3c55b8a7"))
    ap.add_argument("--bundle", default=os.path.join(here, "bundle_config.py"))
    a = ap.parse_args()
    batches = load_sql_queries(a.bundle)
    ok = total = 0
    for bi, batch in enumerate(batches):
        for stmt in batch:
            total += 1
            ok += run(stmt, a.catalog, a.schema, a.warehouse)
    print(f"ran {ok}/{total} statements into {a.catalog}.{a.schema}")
    sys.exit(0 if ok == total else 1)
