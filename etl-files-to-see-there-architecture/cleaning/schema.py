"""
Star schema entity definitions: expected columns, types, business rules, and constraints.
Single source of truth for validation and type casting (SQL Server–compatible).
"""
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Entity schema definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntitySchema:
    """Schema definition for one staging entity (source CSV → clean DataFrame)."""

    name: str
    source_table: str  # Key in TABLE_CSV_MAP (e.g. "Region", "SalesHeader")
    key_columns: tuple[str, ...]  # Natural key / PK; nulls → row dropped
    required_columns: tuple[str, ...]  # Must exist in CSV
    optional_columns: tuple[str, ...] = ()  # May be missing; filled with None
    string_columns: tuple[str, ...] = ()  # Trim + truncate to max_len
    max_lengths: dict = field(default_factory=dict)  # col -> max char length
    numeric_columns: tuple[str, ...] = ()
    integer_columns: tuple[str, ...] = ()
    date_columns: tuple[str, ...] = ()
    business_rules: tuple[dict[str, Any], ...] = ()  # {"column", "op", "value"}


# ---------------------------------------------------------------------------
# Dimension entities (source CSV column names)
# ---------------------------------------------------------------------------

REGION = EntitySchema(
    name="DimRegion",
    source_table="Region",
    key_columns=("regionid",),
    required_columns=("regionid",),
    optional_columns=("description",),
    string_columns=("regionid", "description"),
    max_lengths={"regionid": 50, "description": 100},
)

SECTOR = EntitySchema(
    name="DimSector",
    source_table="Sector",
    key_columns=("sectorid",),
    required_columns=("sectorid",),
    optional_columns=("description",),
    string_columns=("sectorid", "description"),
    max_lengths={"sectorid": 50, "description": 100},
)

CUSTOMER = EntitySchema(
    name="DimCustomer",
    source_table="Customer",
    key_columns=("accountid",),
    required_columns=("accountid",),
    optional_columns=("accountname", "regionid", "sectorid"),
    string_columns=("accountid", "accountname", "regionid", "sectorid"),
    max_lengths={"accountid": 50, "accountname": 100, "regionid": 50, "sectorid": 50},
)

SELLER = EntitySchema(
    name="DimSeller",
    source_table="Seller",
    key_columns=("sellerid",),
    required_columns=("sellerid",),
    optional_columns=("sellername",),
    string_columns=("sellerid", "sellername"),
    max_lengths={"sellerid": 50, "sellername": 100},
)

PRODUCT = EntitySchema(
    name="DimProduct",
    source_table="Product",
    key_columns=("itemid",),
    required_columns=("itemid",),
    optional_columns=("name", "namealias", "marque"),
    string_columns=("itemid", "name", "namealias", "marque"),
    max_lengths={"itemid": 50, "name": 100, "namealias": 100, "marque": 50},
)

# ---------------------------------------------------------------------------
# Fact entities
# ---------------------------------------------------------------------------

SALES_HEADER = EntitySchema(
    name="FactOrders",
    source_table="SalesHeader",
    key_columns=("saleid",),
    required_columns=("saleid", "accountid", "sellerid", "orderdate"),
    optional_columns=("delivdate", "bruteamount", "netamount", "taxamount", "totalamount"),
    string_columns=("accountid", "sellerid"),
    max_lengths={"accountid": 50, "sellerid": 50},
    numeric_columns=("bruteamount", "netamount", "taxamount", "totalamount"),
    date_columns=("orderdate", "delivdate"),
)

SALES_LINE = EntitySchema(
    name="FactSales",
    source_table="SalesLine",
    key_columns=("saleid", "itemid"),
    required_columns=("saleid", "itemid", "qty"),
    optional_columns=("unitprice", "httotalamount", "ttctotalamount", "promotype", "promovalue"),
    string_columns=("itemid", "promotype"),
    max_lengths={"itemid": 50, "promotype": 50},
    numeric_columns=("unitprice", "httotalamount", "ttctotalamount", "promovalue"),
    integer_columns=("qty",),
    business_rules=(
        {"column": "qty", "op": ">", "value": 0},
    ),
)

INVOICE = EntitySchema(
    name="FactPayments",
    source_table="Invoice",
    key_columns=("invoiceid",),
    required_columns=("invoiceid", "salesid"),
    optional_columns=("paymentamount", "paymentmethod"),
    string_columns=("invoiceid", "paymentmethod"),
    max_lengths={"invoiceid": 50, "paymentmethod": 10},
    numeric_columns=("paymentamount",),
)

# ---------------------------------------------------------------------------
# RI: which dimension keys each fact depends on (natural key column names)
# ---------------------------------------------------------------------------

FACT_DEPENDENCIES = {
    "FactOrders": [("accountid", "DimCustomer"), ("sellerid", "DimSeller")],
    "FactSales": [
        ("saleid", "FactOrders"),
        ("accountid", "DimCustomer"),
        ("sellerid", "DimSeller"),
        ("itemid", "DimProduct"),
    ],
    "FactPayments": [("salesid", "FactOrders"), ("accountid", "DimCustomer")],
}

# Sentinel for unknown dimension member (natural key value)
UNKNOWN_NATURAL_KEY = "__UNKNOWN__"
