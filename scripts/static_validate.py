from pathlib import Path
import csv
import ast

ROOT = Path(__file__).resolve().parents[1]

required = [
    "databricks.yml",
    "resources/retail_lakehouse.pipeline.yml",
    "resources/retail_lakehouse.job.yml",
    "src/pipeline/retail_lakehouse.py",
    "src/notebooks/validate_outputs.py",
    "sql/00_setup_unity_catalog.sql",
    "sql/20_validation.sql",
    "sql/30_analytics.sql",
    "docs/ARCHITECTURE.md",
    "docs/DATA_MODEL.md",
    "docs/DASHBOARD_SPEC.md",
    "docs/ACCEPTANCE_CHECKLIST.md",
    "data/customers.csv",
    "data/products.csv",
    "data/orders.csv",
    "data/order_items.csv",
]

missing = [path for path in required if not (ROOT / path).exists()]
if missing:
    raise SystemExit(f"Missing required files: {missing}")

for relative in [
    "src/pipeline/retail_lakehouse.py",
    "src/notebooks/validate_outputs.py",
]:
    ast.parse((ROOT / relative).read_text(encoding="utf-8"))

for relative in [
    "data/customers.csv",
    "data/products.csv",
    "data/orders.csv",
    "data/order_items.csv",
]:
    with (ROOT / relative).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        if not rows:
            raise SystemExit(f"{relative} contains no data")

print("Static repository validation passed.")
