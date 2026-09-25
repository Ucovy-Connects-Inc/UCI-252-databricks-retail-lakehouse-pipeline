from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def read_csv(name):
    with (DATA / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_sample_files_have_rows():
    for name in ["customers.csv", "products.csv", "orders.csv", "order_items.csv"]:
        assert read_csv(name)


def test_customer_ids_unique():
    rows = read_csv("customers.csv")
    ids = [row["customer_id"] for row in rows]
    assert len(ids) == len(set(ids))


def test_product_ids_unique():
    rows = read_csv("products.csv")
    ids = [row["product_id"] for row in rows]
    assert len(ids) == len(set(ids))


def test_order_ids_unique():
    rows = read_csv("orders.csv")
    ids = [row["order_id"] for row in rows]
    assert len(ids) == len(set(ids))


def test_order_item_ids_unique():
    rows = read_csv("order_items.csv")
    ids = [row["order_item_id"] for row in rows]
    assert len(ids) == len(set(ids))
