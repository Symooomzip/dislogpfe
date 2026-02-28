# Data Profiling Report — Dislog PFE

**Generated**: 2026-02-27 19:03:19

---

## File Overview

| Table | File | Size | Encoding |
|-------|------|------|----------|
| Region | Region.csv | 1.8 KB | utf-8 |
| Sector | Sector.csv | 8.0 KB | utf-8 |
| Customer | Customer.csv | 3.8 MB | utf-8 |
| Seller | Seller.csv | 8.9 KB | utf-8 |
| Product | Products.csv | 142.3 KB | utf-8 |
| SalesHeader | SalesHeader.csv | 117.6 MB | utf-8 |
| SalesLine | SalesLine.csv | 686.1 MB | cp1252 |
| Invoice | Invoice.csv | 62.7 MB | cp1252 |


---

## Region

**Rows**: 80  
**Columns**: 2

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `regionid` | object | 79 | 1 | 1.2% |
| `description` | object | 77 | 1 | 1.2% |

✅ No duplicate rows

### Sample Data

```
        regionid      description
0  Casa Ben Msik    Casa Ben Msik
1  CASA PARF HFS    CASA PARF HFS
2    FD_Casa-Sud  Casa-Sud-HFS-FD
```

### ✅ No major quality issues detected


---

## Sector

❌ **ERROR**: 'utf-8' codec can't decode byte 0xe9 in position 1: invalid continuation byte


---

## Customer

**Rows**: 87,668  
**Columns**: 4

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `accountid` | object | 87,652 | 0 | 0.0% |
| `accountname` | object | 46,946 | 10,341 | 11.8% |
| `regionid` | object | 93 | 192 | 0.2% |
| `sectorid` | object | 322 | 317 | 0.4% |

⚠ **5 duplicate rows** (0.01%)

### Sample Data

```
      accountid      accountname      regionid sectorid
0  109309510653  Mostafa Boukili  FD_Marrakech   FDS093
1  109309510659              Ali  FD_Marrakech   FDS093
2  109309510671          Mohamed  FD_Marrakech   FDS093
```

### ⚠ Data Quality Issues

- Column `accountname`: 2,138 values with leading/trailing whitespace


---

## Seller

**Rows**: 409  
**Columns**: 2

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `sellerid` | object | 409 | 0 | 0.0% |
| `sellername` | object | 399 | 0 | 0.0% |

✅ No duplicate rows

### Sample Data

```
  sellerid         sellername
0     6169  ABDELFATTAH ANIBA
1     7311  SALAH EZ-ZARROUQY
2     7325     FOUAD LAMCHICH
```

### ⚠ Data Quality Issues

- Column `sellername`: 4 values with leading/trailing whitespace


---

## Product

**Rows**: 2,319  
**Columns**: 4

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `itemid` | object | 2,319 | 0 | 0.0% |
| `name` | object | 2,211 | 0 | 0.0% |
| `namealias` | object | 2,167 | 0 | 0.0% |
| `marque` | object | 70 | 3 | 0.1% |

✅ No duplicate rows

### Sample Data

```
           itemid                       name           namealias   marque
0  11111230215815            Crayon HL Wood8       CrayonHLWood8  YAN&ONE
1  11111240410132                Mascara XXL          MascaraXXL  YAN&ONE
2  11111250202352  Poudre sourcils CoffeeK2g  PoudresourcilsCoff  YAN&ONE
```

### ⚠ Data Quality Issues

- Column `name`: 5 values with leading/trailing whitespace
- Column `namealias`: 1 values with leading/trailing whitespace


---

## SalesHeader

**Rows**: 1,448,981 (profiled on first 100K rows)  
**Columns**: 9

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `saleid` | int64 | 100,000 | 0 | 0.0% |
| `accountid` | object | 9,271 | 0 | 0.0% |
| `sellerid` | int64 | 70 | 0 | 0.0% |
| `orderdate` | object | 362 | 0 | 0.0% |
| `delivdate` | object | 349 | 0 | 0.0% |
| `bruteamount` | float64 | 52,791 | 0 | 0.0% |
| `netamount` | float64 | 56,757 | 0 | 0.0% |
| `taxamount` | float64 | 34,197 | 0 | 0.0% |
| `totalamount` | float64 | 58,759 | 0 | 0.0% |

✅ No duplicate rows

### Numeric Statistics

```
             saleid    sellerid  bruteamount    netamount   taxamount  totalamount
count  1.000000e+05  100000.000   100000.000   100000.000  100000.000   100000.000
mean   1.262578e+13    1095.335     6112.963     5402.342    1077.525     6488.311
std    1.035624e+13    1293.387    42118.619    36919.403    7383.837    44372.806
min    1.020423e+13     101.000        0.000        0.000       0.000        0.000
25%    1.101123e+13    1011.000      428.197      410.053      79.632      490.308
50%    1.101623e+13    1016.000      834.232      788.445     156.268      947.156
75%    1.122823e+13    1225.000     1455.440     1380.656     273.662     1656.876
max    1.111013e+15   11219.000  2336600.000  2106678.532  421335.726  2534334.250
```

### Sample Data

```
           saleid accountid  sellerid   orderdate   delivdate  bruteamount  netamount  taxamount  totalamount
0  11011230008618   ER01012      1011  2024-07-10  2024-07-11     1086.540   1026.872    205.366     1235.318
1  11011230008621   ER01013      1011  2024-07-10  2024-07-11      660.954    620.494    124.096      746.452
2  11011230008622   ER01014      1011  2024-07-10  2024-07-11     1105.608   1046.094    209.216     1258.446
```

### ⚠ Data Quality Issues

- Column `accountid`: 6 values with leading/trailing whitespace


---

## SalesLine

**Rows**: 10,577,126 (profiled on first 100K rows)  
**Columns**: 8

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `saleid` | int64 | 17,081 | 0 | 0.0% |
| `itemid` | object | 269 | 0 | 0.0% |
| `qty` | float64 | 242 | 0 | 0.0% |
| `unitprice` | float64 | 170 | 0 | 0.0% |
| `httotalamount` | float64 | 2,798 | 0 | 0.0% |
| `ttctotalamount` | float64 | 2,805 | 0 | 0.0% |
| `promotype` | object | 3 | 0 | 0.0% |
| `promovalue` | float64 | 79 | 0 | 0.0% |

⚠ **1,909 duplicate rows** (0.02%)

### Numeric Statistics

```
             saleid         qty   unitprice  httotalamount  ttctotalamount  promovalue
count  1.000000e+05  100000.000  100000.000     100000.000      100000.000  100000.000
mean   1.020779e+13      39.434      17.731        238.125         285.237       4.607
std    2.339278e+09     291.720      31.985        481.521         576.721       7.464
min    1.020423e+13       1.000       0.000          0.000           0.000       0.000
25%    1.020623e+13       2.000       0.000          0.000           0.000       0.000
50%    1.020723e+13       9.000       9.996        113.134         135.758       2.000
75%    1.021123e+13      24.000      18.676        277.606         333.130       6.700
max    1.021423e+13   24000.000     355.012      50309.280       60371.136     832.000
```

### Sample Data

```
           saleid         itemid   qty  unitprice  httotalamount  ttctotalamount     promotype  promovalue
0  10204230001247  8001090264930  36.0     18.676        672.336         806.806  No Promotion         0.0
1  10204230001247  8006540760963  16.0      8.260        132.160         158.592  No Promotion         0.0
2  10204230001247  8001090347626  60.0     18.228       1093.680        1312.416  No Promotion         0.0
```

### ✅ No major quality issues detected


---

## Invoice

**Rows**: 1,448,980  
**Columns**: 4

### Columns

| Column | Dtype | Unique | Missing | Missing % |
|--------|-------|--------|---------|-----------|
| `invoiceid` | object | 1,448,836 | 0 | 0.0% |
| `salesid` | object | 1,448,980 | 0 | 0.0% |
| `paymentamount` | float64 | 264,375 | 0 | 0.0% |
| `paymentmethod` | int64 | 3 | 0 | 0.0% |

✅ No duplicate rows

### Numeric Statistics

```
       paymentamount  paymentmethod
count    1448980.000    1448980.000
mean        2541.804        102.668
std        19721.872         83.319
min            0.000          1.000
25%          366.296          1.000
50%          760.340        102.000
75%         1303.036        205.000
max      7901600.000        205.000
```

### Sample Data

```
          invoiceid         salesid  paymentamount  paymentmethod
0  24-CMD-DJ2802269  11011230008618       1235.318              1
1  24-CMD-UH5614117  11011230008621        746.452            205
2  24-CMD-HW3423956  11011230008622       1258.446            205
```

### ✅ No major quality issues detected


---

## Summary

### Key Findings

- All CSV files use semicolon (;) as delimiter
- SalesLine.csv and Invoice.csv use ANSI (cp1252) encoding
- Invoice.csv uses comma (,) as decimal separator instead of period (.)
- SalesHeader saleid is a long numeric string, not a simple integer

**Total data rows**: 13,565,563

## Referential Integrity

- ❌ Error: 'utf-8' codec can't decode byte 0xe9 in position 293: invalid continuation byte

## Date Range

- **Order dates**: 2024-01-01 00:00:00 → 2024-12-31 00:00:00
- **Delivery dates**: 2024-01-02 00:00:00 → 2025-01-22 00:00:00
- ⚠ 1 deliveries before order date

## Promotion Types

| Promotion Type | Count | % |
|----------------|-------|---|
| Discount | 210,959 | 42.2% |
| Gift | 158,719 | 31.7% |
| No Promotion | 93,436 | 18.7% |
| Rebates | 36,886 | 7.4% |

## Payment Methods

| Method | Count | % | Avg Amount |
|--------|-------|---|------------|
- ❌ Error: Unknown format code 's' for object of type 'int'

## Negative Amounts

- ✅ SalesHeader.`bruteamount`: no negatives
- ✅ SalesHeader.`netamount`: no negatives
- ✅ SalesHeader.`taxamount`: no negatives
- ✅ SalesHeader.`totalamount`: no negatives