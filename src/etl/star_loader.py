"""
Star schema loader: insert cleaned DataFrames into Dim* and Fact* (SQL Server).
Assumes StarSchema.sql has been applied. Load order: dimensions first, then facts.
"""
import logging
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# SQL Server allows max 2100 parameters per batch. FactSales has 15 columns -> max 140 rows/chunk.
CHUNKSIZE = 100

logger = logging.getLogger(__name__)

# Reverse FK order for truncate
STAR_DELETE_ORDER = [
    "FactInvoices",
    "FactSales",
    "DimPaymentMethod",
    "DimPromotion",
    "DimProduct",
    "DimSeller",
    "DimCustomer",
    "DimDate",
]


def truncate_star_schema(engine: Engine) -> None:
    with engine.connect() as conn:
        for table in STAR_DELETE_ORDER:
            try:
                conn.execute(text(f"DELETE FROM [{table}]"))
                conn.commit()
                logger.info("Cleared table %s", table)
            except Exception as e:
                logger.debug("Clear %s: %s", table, e)
                conn.rollback()


def _date_to_key(d: Any) -> int | None:
    if d is None:
        return None
    try:
        if pd.isna(d):
            return None
    except (TypeError, ValueError):
        pass
    try:
        ts = pd.to_datetime(d)
        return int(ts.strftime("%Y%m%d"))
    except (TypeError, ValueError, AttributeError):
        return None


def load_dim_date(engine: Engine, df: pd.DataFrame) -> None:
    """Load DimDate. df must have DateKey, FullDate, Year, Quarter, Month, Day, DayOfWeek, DayName, MonthName, IsWeekend."""
    if df.empty:
        return
    df = df.copy()
    if "IsWeekend" in df.columns and df["IsWeekend"].dtype == float:
        df["IsWeekend"] = df["IsWeekend"].fillna(0).astype(int)
    df.to_sql("DimDate", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    logger.info("Loaded %s rows into DimDate", len(df))


def load_dim_promotion(engine: Engine, promo_types: pd.Series) -> dict[str, int]:
    """Build DimPromotion from distinct promotype; returns PromoType -> PromotionKey."""
    if promo_types.empty:
        return {}
    distinct = promo_types.dropna().astype(str).str.strip().unique()
    rows = [{"PromoType": p} for p in distinct if p]
    if not rows:
        return {}
    df = pd.DataFrame(rows).drop_duplicates(subset=["PromoType"])
    df.to_sql("DimPromotion", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT PromoType, PromotionKey FROM DimPromotion"))
        rows = result.fetchall()
    return {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}


def load_dim_payment_method(engine: Engine, payment_codes: pd.Series) -> dict[str, int]:
    """Build DimPaymentMethod from distinct paymentmethod; returns code (str) -> PaymentMethodKey."""
    if payment_codes.empty:
        return {}
    distinct = payment_codes.dropna().astype(str).str.strip().unique()
    rows = [{"PaymentMethodCode": p, "PaymentMethodDescription": p} for p in distinct if p]
    if not rows:
        return {}
    df = pd.DataFrame(rows).drop_duplicates(subset=["PaymentMethodCode"])
    df.to_sql("DimPaymentMethod", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT PaymentMethodCode, PaymentMethodKey FROM DimPaymentMethod"))
        rows = result.fetchall()
    return {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}


def get_customer_lookup_from_db(engine: Engine) -> dict[str, int]:
    """Build AccountID -> CustomerKey from existing DimCustomer (for resume / partial load)."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT AccountID, CustomerKey FROM DimCustomer"))
        rows = result.fetchall()
    return {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}


def get_payment_method_lookup_from_db(engine: Engine) -> dict[str, int]:
    """Build PaymentMethodCode -> PaymentMethodKey from existing DimPaymentMethod (for resume / partial load)."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT PaymentMethodCode, PaymentMethodKey FROM DimPaymentMethod"))
        rows = result.fetchall()
    return {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}


def load_dim_customer(
    engine: Engine,
    df_customer: pd.DataFrame,
    df_region: pd.DataFrame,
    df_sector: pd.DataFrame,
) -> dict[str, int]:
    """Load DimCustomer. Customer + Region (regionid -> description) + Sector (sectorid -> description). Returns AccountID -> CustomerKey."""
    if df_customer.empty:
        return {}
    df = df_customer.copy()
    region_map = df_region.set_index("regionid")["description"].to_dict() if not df_region.empty else {}
    sector_map = df_sector.set_index("sectorid")["description"].to_dict() if not df_sector.empty else {}
    df["RegionDescription"] = df["regionid"].astype(str).str.strip().map(region_map)
    df["SectorDescription"] = df["sectorid"].astype(str).str.strip().map(sector_map)
    out = df[["accountid", "accountname", "regionid", "RegionDescription", "sectorid", "SectorDescription"]].copy()
    out = out.rename(columns={
        "accountid": "AccountID",
        "accountname": "AccountName",
        "regionid": "RegionID",
        "sectorid": "SectorID",
    })
    out = out.drop_duplicates(subset=["AccountID"])
    out.to_sql("DimCustomer", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT AccountID, CustomerKey FROM DimCustomer"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimCustomer", len(out))
    return lookup


def load_dim_seller(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    """Returns SellerID (str) -> SellerKey."""
    if df.empty:
        return {}
    out = df[["sellerid", "sellername"]].drop_duplicates(subset=["sellerid"]).copy()
    out = out.rename(columns={"sellerid": "SellerID", "sellername": "SellerName"})
    out.to_sql("DimSeller", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT SellerID, SellerKey FROM DimSeller"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimSeller", len(out))
    return lookup


def load_dim_product(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    """Returns ItemID (str) -> ProductKey."""
    if df.empty:
        return {}
    out = df[["itemid", "name", "namealias", "marque"]].drop_duplicates(subset=["itemid"]).copy()
    out = out.rename(columns={"itemid": "ItemID", "name": "ProductName", "namealias": "NameAlias", "marque": "Brand"})
    out.to_sql("DimProduct", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT ItemID, ProductKey FROM DimProduct"))
        rows = result.fetchall()
    lookup = {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    logger.info("Loaded %s rows into DimProduct", len(out))
    return lookup


def load_fact_sales(
    engine: Engine,
    df_header: pd.DataFrame,
    df_line: pd.DataFrame,
    customer_lookup: dict[str, int],
    seller_lookup: dict[str, int],
    product_lookup: dict[str, int],
    promotion_lookup: dict[str, int],
) -> None:
    """
    FactSales at line grain: join SalesLine to SalesHeader; resolve all FKs; line-level measures only.
    LineBruteAmount=httotalamount, LineNetAmount=httotalamount, LineDiscountAmount=promovalue,
    LineTaxAmount=ttctotalamount-httotalamount, LineTotalAmount=ttctotalamount.
    """
    if df_line.empty or df_header.empty:
        return
    header_cols = ["saleid", "accountid", "sellerid", "orderdate", "delivdate"]
    h = df_header[header_cols].drop_duplicates(subset=["saleid"])
    df = df_line.merge(h, on="saleid", how="inner")

    df["OrderDateKey"] = df["orderdate"].apply(_date_to_key)
    df["DeliveryDateKey"] = df["delivdate"].apply(_date_to_key)
    df["CustomerKey"] = df["accountid"].astype(str).str.strip().map(customer_lookup)
    df["SellerKey"] = df["sellerid"].astype(str).str.strip().map(seller_lookup)
    df["ProductKey"] = df["itemid"].astype(str).str.strip().map(product_lookup)
    df["PromotionKey"] = df["promotype"].astype(str).str.strip().map(promotion_lookup)

    df = df.dropna(subset=["OrderDateKey", "DeliveryDateKey", "CustomerKey", "SellerKey", "ProductKey", "PromotionKey"])

    htt = df["httotalamount"] if "httotalamount" in df.columns else pd.Series(0.0, index=df.index)
    ttc = df["ttctotalamount"] if "ttctotalamount" in df.columns else htt
    promo = df["promovalue"] if "promovalue" in df.columns else pd.Series(0.0, index=df.index)

    out = pd.DataFrame({
        "SaleID": pd.to_numeric(df["saleid"], errors="coerce").astype("Int64").fillna(0).astype("int64"),
        "OrderDateKey": df["OrderDateKey"].astype(int),
        "DeliveryDateKey": df["DeliveryDateKey"].astype(int),
        "CustomerKey": df["CustomerKey"].astype(int),
        "SellerKey": df["SellerKey"].astype(int),
        "ProductKey": df["ProductKey"].astype(int),
        "PromotionKey": df["PromotionKey"].astype(int),
        "Quantity": pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int),
        "UnitPrice": pd.to_numeric(df["unitprice"], errors="coerce").fillna(0),
        "LineBruteAmount": htt,
        "LineDiscountAmount": promo,
        "LineNetAmount": htt,
        "LineTaxAmount": (ttc - htt).fillna(0),
        "LineTotalAmount": ttc,
    })
    out = out.dropna(subset=["OrderDateKey", "DeliveryDateKey", "CustomerKey", "SellerKey", "ProductKey", "PromotionKey"])
    out.to_sql("FactSales", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    logger.info("Loaded %s rows into FactSales", len(out))


def load_fact_invoices(
    engine: Engine,
    df_invoice: pd.DataFrame,
    df_header: pd.DataFrame,
    customer_lookup: dict[str, int],
    payment_method_lookup: dict[str, int],
) -> None:
    """FactInvoices: lookup CustomerKey and PaymentDateKey from SalesHeader by saleid (map-based to avoid merge blow-up)."""
    if df_invoice.empty or df_header.empty:
        return
    # Build saleid -> (accountid, orderdate) so we avoid a merge (merge on NaN keys blows up to 100M+ rows)
    h = df_header[["saleid", "accountid", "orderdate"]].drop_duplicates(subset=["saleid"])
    h["saleid"] = pd.to_numeric(h["saleid"], errors="coerce")
    h = h.dropna(subset=["saleid"])
    saleid_to_header = dict(zip(h["saleid"].astype("int64"), zip(h["accountid"].astype(str).str.strip(), h["orderdate"])))

    df = df_invoice.copy()
    df["salesid_clean"] = pd.to_numeric(df["salesid"], errors="coerce")
    df = df.dropna(subset=["salesid_clean"])
    df["salesid_clean"] = df["salesid_clean"].astype("int64")

    # Lookup (accountid, orderdate) for each invoice
    lookup = df["salesid_clean"].map(saleid_to_header)
    df["_accountid"] = lookup.map(lambda x: x[0] if x else None)
    df["_orderdate"] = lookup.map(lambda x: x[1] if x else None)

    df = df.dropna(subset=["_accountid", "_orderdate"])
    df["PaymentDateKey"] = df["_orderdate"].apply(_date_to_key)
    df["CustomerKey"] = df["_accountid"].str.strip().map(customer_lookup)
    df["PaymentMethodKey"] = df["paymentmethod"].astype(str).str.strip().map(payment_method_lookup)

    df = df.dropna(subset=["PaymentDateKey", "CustomerKey", "PaymentMethodKey"])

    out = pd.DataFrame({
        "InvoiceID": df["invoiceid"].astype(str),
        "SaleID": df["salesid_clean"],
        "PaymentDateKey": df["PaymentDateKey"].astype(int),
        "CustomerKey": df["CustomerKey"].astype(int),
        "PaymentMethodKey": df["PaymentMethodKey"].astype(int),
        "PaymentAmount": pd.to_numeric(df["paymentamount"], errors="coerce").fillna(0),
    })
    out.to_sql("FactInvoices", engine, if_exists="append", index=False, method="multi", chunksize=CHUNKSIZE)
    logger.info("Loaded %s rows into FactInvoices", len(out))
