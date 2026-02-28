"""
Star schema loader: insert cleaned DataFrames into Dim* and Fact* with surrogate key lookups.
Used by run_star_etl.py. Assumes Schema_Star.sql has been applied to the database.
"""
import logging
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# SQL Server parameter limit for batch inserts
CHUNKSIZE = 100

logger = logging.getLogger(__name__)

# Reverse dependency order for truncate/delete
STAR_DELETE_ORDER = [
    "FactPayments",
    "FactSales",
    "FactOrders",
    "DimProduct",
    "DimSeller",
    "DimCustomer",
    "DimSector",
    "DimRegion",
    "DimDate",
]


def truncate_star_schema(engine: Engine) -> None:
    """Delete all data from star schema tables in reverse FK order."""
    with engine.connect() as conn:
        for table in STAR_DELETE_ORDER:
            try:
                conn.execute(text(f"DELETE FROM [{table}]"))
                conn.commit()
                logger.info("Cleared table %s", table)
            except Exception as e:
                logger.debug("Clear %s: %s", table, e)
                conn.rollback()


def load_dim_date(engine: Engine, df: pd.DataFrame) -> None:
    """Load DimDate. df must have columns: date_key, full_date, year, quarter, month, month_name, week_of_year, day_of_month, day_of_week, day_name, is_weekend."""
    if df.empty:
        return
    df = df.copy()
    if "is_weekend" in df.columns and df["is_weekend"].dtype == float:
        df["is_weekend"] = df["is_weekend"].fillna(0).astype(int)
    df.to_sql("DimDate", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    logger.info("Loaded %s rows into DimDate", len(df))


def load_dim_region(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    """Load DimRegion. df has regionid, description. Map to region_id, region_name. Returns region_id -> region_key."""
    if df.empty:
        return {}
    df = df.copy()
    df = df.rename(columns={"regionid": "region_id", "description": "region_name"})
    df = df[["region_id", "region_name"]].drop_duplicates(subset=["region_id"])
    df.to_sql("DimRegion", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT region_id, region_key FROM DimRegion"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimRegion", len(df))
    return lookup


def load_dim_sector(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    """Load DimSector. df has sectorid, description. Returns sector_id -> sector_key."""
    if df.empty:
        return {}
    df = df.copy()
    df = df.rename(columns={"sectorid": "sector_id", "description": "sector_name"})
    df = df[["sector_id", "sector_name"]].drop_duplicates(subset=["sector_id"])
    df.to_sql("DimSector", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT sector_id, sector_key FROM DimSector"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimSector", len(df))
    return lookup


def load_dim_customer(
    engine: Engine,
    df: pd.DataFrame,
    region_lookup: dict[str, int],
    sector_lookup: dict[str, int],
) -> dict[str, int]:
    """Load DimCustomer. df has accountid, accountname, regionid, sectorid. Resolve region_key, sector_key. Returns accountid -> customer_key."""
    if df.empty:
        return {}
    df = df.copy()
    df["region_key"] = df["regionid"].astype(str).str.strip().map(region_lookup)
    df["sector_key"] = df["sectorid"].astype(str).str.strip().map(sector_lookup)
    out = df[["accountid", "accountname", "region_key", "sector_key"]].drop_duplicates(subset=["accountid"])
    out.to_sql("DimCustomer", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT accountid, customer_key FROM DimCustomer"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimCustomer", len(out))
    return lookup


def load_dim_seller(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    """Load DimSeller. Returns sellerid -> seller_key."""
    if df.empty:
        return {}
    df = df.copy()
    out = df[["sellerid", "sellername"]].drop_duplicates(subset=["sellerid"])
    out.to_sql("DimSeller", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT sellerid, seller_key FROM DimSeller"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimSeller", len(out))
    return lookup


def load_dim_product(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    """Load DimProduct. Returns itemid -> product_key."""
    if df.empty:
        return {}
    df = df.copy()
    out = df[["itemid", "name", "namealias", "marque"]].drop_duplicates(subset=["itemid"])
    out.to_sql("DimProduct", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT itemid, product_key FROM DimProduct"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimProduct", len(out))
    return lookup


def _date_to_key(d: pd.Timestamp | str | None) -> int | None:
    if d is None or pd.isna(d):
        return None
    if isinstance(d, str):
        d = pd.to_datetime(d)
    return int(d.strftime("%Y%m%d"))


def load_fact_orders(
    engine: Engine,
    df: pd.DataFrame,
    customer_lookup: dict[str, int],
    seller_lookup: dict[str, int],
) -> dict[int, int]:
    """
    Load FactOrders. df has saleid, accountid, sellerid, orderdate, delivdate, bruteamount, netamount, taxamount, totalamount.
    date_key is computed from orderdate (YYYYMMDD); DimDate must already be populated for that range.
    Returns order_id (saleid) -> order_key.
    """
    if df.empty:
        return {}
    df = df.copy()
    df["date_key"] = df["orderdate"].apply(_date_to_key)
    df["delivery_date_key"] = df["delivdate"].apply(_date_to_key)
    df["customer_key"] = df["accountid"].astype(str).str.strip().map(customer_lookup)
    df["seller_key"] = df["sellerid"].astype(str).str.strip().map(seller_lookup)
    # delivery_days
    order_dt = pd.to_datetime(df["orderdate"], errors="coerce")
    deliv_dt = pd.to_datetime(df["delivdate"], errors="coerce")
    df["delivery_days"] = (deliv_dt - order_dt).dt.days
    out = df[
        [
            "saleid", "date_key", "customer_key", "seller_key",
            "bruteamount", "netamount", "taxamount", "totalamount",
            "delivery_date_key", "delivery_days",
        ]
    ].copy()
    out = out.dropna(subset=["date_key", "customer_key", "seller_key"])
    out = out.rename(columns={
        "saleid": "order_id",
        "bruteamount": "brute_amount",
        "netamount": "net_amount",
        "taxamount": "tax_amount",
        "totalamount": "total_amount",
    })
    out = out[
        [
            "order_id", "date_key", "customer_key", "seller_key",
            "brute_amount", "net_amount", "tax_amount", "total_amount",
            "delivery_date_key", "delivery_days",
        ]
    ]
    # Cast to match SQL Server schema types (BIGINT/INT)
    out["order_id"] = pd.to_numeric(out["order_id"], errors="coerce")
    out = out.dropna(subset=["order_id"])
    out["order_id"] = out["order_id"].astype("int64")
    out["date_key"] = out["date_key"].astype(int)
    out["customer_key"] = out["customer_key"].astype(int)
    out["seller_key"] = out["seller_key"].astype(int)
    out["delivery_date_key"] = pd.to_numeric(out["delivery_date_key"], errors="coerce").astype("Int64")
    out["delivery_days"] = pd.to_numeric(out["delivery_days"], errors="coerce").astype("Int64")
    out.to_sql("FactOrders", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT order_id, order_key FROM FactOrders"))
        rows = result.fetchall()
    lookup = {int(r[0]): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into FactOrders", len(out))
    return lookup


def get_fact_orders_lookups(engine: Engine) -> tuple[dict[int, int], dict[int, tuple[int, int]]]:
    """
    After FactOrders is loaded, return (order_id -> order_key, order_id -> (customer_key, date_key))
    for use by FactSales (order_id) and FactPayments (order_key, customer_key, date_key).
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT order_id, order_key, customer_key, date_key FROM FactOrders"))
        rows = result.fetchall()
    order_key_lookup = {int(r[0]): int(r[1]) for r in rows if r[0] is not None}
    order_customer_date = {int(r[0]): (int(r[2]), int(r[3])) for r in rows if r[0] is not None}
    return order_key_lookup, order_customer_date


def load_fact_sales(
    engine: Engine,
    df_sales: pd.DataFrame,
    df_orders: pd.DataFrame,
    order_key_lookup: dict[int, int],
    customer_lookup: dict[str, int],
    seller_lookup: dict[str, int],
    product_lookup: dict[str, int],
) -> None:
    """
    Load FactSales. df_sales has saleid, itemid, qty, unitprice, httotalamount, ttctotalamount, promotype, promovalue.
    Resolve order_key from saleid via order_key_lookup; date_key, customer_key, seller_key from the order.
    FactSales references FactOrders(order_key), not order_id.
    """
    if df_sales.empty:
        return
    df = df_sales.copy()
    orders_min = df_orders[["saleid", "accountid", "sellerid", "orderdate"]].drop_duplicates(subset=["saleid"])
    df = df.merge(orders_min, left_on="saleid", right_on="saleid", how="inner")
    df["saleid_int"] = pd.to_numeric(df["saleid"], errors="coerce")
    df = df.dropna(subset=["saleid_int"])
    df["saleid_int"] = df["saleid_int"].astype("int64")
    df["order_key"] = df["saleid_int"].map(order_key_lookup)
    df = df.dropna(subset=["order_key"])
    df["date_key"] = df["orderdate"].apply(_date_to_key)
    df["customer_key"] = df["accountid"].astype(str).str.strip().map(customer_lookup)
    df["seller_key"] = df["sellerid"].astype(str).str.strip().map(seller_lookup)
    df["product_key"] = df["itemid"].astype(str).str.strip().map(product_lookup)
    df = df.dropna(subset=["date_key", "customer_key", "seller_key", "product_key"])
    out = pd.DataFrame({
        "order_key": df["order_key"].astype(int),
        "date_key": df["date_key"].astype(int),
        "customer_key": df["customer_key"].astype(int),
        "seller_key": df["seller_key"].astype(int),
        "product_key": df["product_key"].astype(int),
        "quantity": df["qty"].astype(int),
        "unit_price": df["unitprice"],
        "net_amount": df["httotalamount"],
        "total_amount": df["ttctotalamount"] if "ttctotalamount" in df.columns else df["httotalamount"],
    })
    out["tax_amount"] = out["total_amount"] - out["net_amount"]
    out["promo_type"] = df["promotype"].astype(str) if "promotype" in df.columns else None
    out["promo_value"] = df["promovalue"] if "promovalue" in df.columns else None
    out.to_sql("FactSales", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    logger.info("Loaded %s rows into FactSales", len(out))


def load_fact_payments(
    engine: Engine,
    df: pd.DataFrame,
    order_key_lookup: dict[int, int],
    order_customer_date: dict[int, tuple[int, int]],
) -> None:
    """
    Load FactPayments. df has invoiceid, salesid, paymentamount, paymentmethod.
    We need date_key, customer_key, order_key. order_key from salesid via order_key_lookup.
    order_customer_date maps order_id (saleid) -> (customer_key, date_key).
    """
    if df.empty:
        return
    df = df.copy()
    df["salesid_int"] = pd.to_numeric(df["salesid"], errors="coerce")
    df = df.dropna(subset=["salesid_int"])
    df["salesid_int"] = df["salesid_int"].astype("int64")
    df["order_key"] = df["salesid_int"].map(order_key_lookup)
    df = df.dropna(subset=["order_key"])
    oid = df["salesid_int"]
    df["customer_key"] = oid.map(lambda x: order_customer_date.get(x, (None, None))[0])
    df["date_key"] = oid.map(lambda x: order_customer_date.get(x, (None, None))[1])
    df = df.dropna(subset=["customer_key", "date_key"])
    out = pd.DataFrame({
        "invoice_id": df["invoiceid"].astype(str),
        "date_key": df["date_key"].astype(int),
        "customer_key": df["customer_key"].astype(int),
        "order_key": df["order_key"].astype(int),
        "payment_amount": df["paymentamount"],
        "payment_method": df["paymentmethod"].astype(str) if "paymentmethod" in df.columns else "",
    })
    out.to_sql("FactPayments", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    logger.info("Loaded %s rows into FactPayments", len(out))
