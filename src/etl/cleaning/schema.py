"""
Star schema entity definitions: expected columns, types, business rules.
Single source of truth for validation and type casting (SQL Server–compatible).
"""
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EntitySchema:
    """Schema for one staging entity (source CSV → clean DataFrame)."""

    name: str
    source_table: str
    key_columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    string_columns: tuple[str, ...] = ()
    max_lengths: dict = field(default_factory=dict)
    numeric_columns: tuple[str, ...] = ()
    integer_columns: tuple[str, ...] = ()
    date_columns: tuple[str, ...] = ()
    business_rules: tuple[dict[str, Any], ...] = ()


# Dimensions (source CSV column names)
REGION = EntitySchema(
    name="Region",
    source_table="Region",
    key_columns=("regionid",),
    required_columns=("regionid",),
    optional_columns=("description",),
    string_columns=("regionid", "description"),
    max_lengths={"regionid": 50, "description": 100},
)

SECTOR = EntitySchema(
    name="Sector",
    source_table="Sector",
    key_columns=("sectorid",),
    required_columns=("sectorid",),
    optional_columns=("description",),
    string_columns=("sectorid", "description"),
    max_lengths={"sectorid": 50, "description": 100},
)

CUSTOMER = EntitySchema(
    name="Customer",
    source_table="Customer",
    key_columns=("accountid",),
    required_columns=("accountid",),
    optional_columns=("accountname", "regionid", "sectorid"),
    string_columns=("accountid", "accountname", "regionid", "sectorid"),
    max_lengths={"accountid": 50, "accountname": 100, "regionid": 50, "sectorid": 50},
)

SELLER = EntitySchema(
    name="Seller",
    source_table="Seller",
    key_columns=("sellerid",),
    required_columns=("sellerid",),
    optional_columns=("sellername",),
    string_columns=("sellerid", "sellername"),
    max_lengths={"sellerid": 50, "sellername": 100},
)

PRODUCT = EntitySchema(
    name="Product",
    source_table="Product",
    key_columns=("itemid",),
    required_columns=("itemid",),
    optional_columns=("name", "namealias", "marque"),
    string_columns=("itemid", "name", "namealias", "marque"),
    max_lengths={"itemid": 50, "name": 100, "namealias": 100, "marque": 50},
)

# Facts / transaction tables (cleaned for star loader)
SALES_HEADER = EntitySchema(
    name="SalesHeader",
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
    name="SalesLine",
    source_table="SalesLine",
    key_columns=("saleid", "itemid"),
    required_columns=("saleid", "itemid", "qty"),
    optional_columns=("unitprice", "httotalamount", "ttctotalamount", "promotype", "promovalue"),
    string_columns=("itemid", "promotype"),
    max_lengths={"itemid": 50, "promotype": 50},
    numeric_columns=("unitprice", "httotalamount", "ttctotalamount", "promovalue"),
    integer_columns=("qty",),
    business_rules=({"column": "qty", "op": ">", "value": 0},),
)

INVOICE = EntitySchema(
    name="Invoice",
    source_table="Invoice",
    key_columns=("invoiceid",),
    required_columns=("invoiceid", "salesid"),
    optional_columns=("paymentamount", "paymentmethod"),
    string_columns=("invoiceid", "paymentmethod"),
    max_lengths={"invoiceid": 50, "paymentmethod": 10},
    numeric_columns=("paymentamount",),
)

UNKNOWN_NATURAL_KEY = "__UNKNOWN__"
