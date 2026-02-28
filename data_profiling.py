"""
Data Profiling Script — Dislog PFE
===================================
Explores all 8 raw CSV files and generates a comprehensive profiling report.
Outputs results to console and saves a summary report.

This script handles:
- Semicolon-delimited CSVs
- ANSI (cp1252) encoding for some files
- Comma decimal separators in Invoice.csv
- Large files via chunked reading (SalesLine ~719MB)
"""

import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path
from datetime import datetime

# ──────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent / "Data"
REPORT_PATH = Path(__file__).resolve().parent / "notebooks" / "data_profiling_report.md"

CSV_FILES = {
    "Region":       {"file": "Region.csv",       "encoding": "utf-8"},
    "Sector":       {"file": "Sector.csv",       "encoding": "utf-8"},
    "Customer":     {"file": "Customer.csv",     "encoding": "utf-8"},
    "Seller":       {"file": "Seller.csv",       "encoding": "utf-8"},
    "Product":      {"file": "Products.csv",     "encoding": "utf-8"},
    "SalesHeader":  {"file": "SalesHeader.csv",  "encoding": "utf-8"},
    "SalesLine":    {"file": "SalesLine.csv",    "encoding": "cp1252"},
    "Invoice":      {"file": "Invoice.csv",      "encoding": "cp1252"},
}

# Separator used in all files
SEP = ";"

# ──────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────

def fmt_size(size_bytes):
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def profile_dataframe(df, name):
    """Generate a detailed profile of a DataFrame."""
    report = {}
    report['shape'] = df.shape
    report['columns'] = list(df.columns)
    report['dtypes'] = df.dtypes.to_dict()
    
    # Missing values
    missing = df.isnull().sum()
    missing_pct = (df.isnull().sum() / len(df) * 100).round(2)
    report['missing'] = pd.DataFrame({
        'Missing Count': missing,
        'Missing %': missing_pct,
        'Dtype': df.dtypes
    })
    
    # Unique values
    report['nunique'] = df.nunique()
    
    # Duplicates
    report['duplicate_rows'] = df.duplicated().sum()
    
    # Numeric stats
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        report['numeric_stats'] = df[numeric_cols].describe().round(3)
    else:
        report['numeric_stats'] = None
    
    # Sample values
    report['head'] = df.head(3)
    report['tail'] = df.tail(3)
    
    # Specific checks
    # Check for leading/trailing whitespace in string columns
    str_cols = df.select_dtypes(include=['object']).columns
    whitespace_issues = {}
    for col in str_cols:
        non_null = df[col].dropna()
        if len(non_null) > 0:
            ws_count = (non_null != non_null.str.strip()).sum()
            if ws_count > 0:
                whitespace_issues[col] = ws_count
    report['whitespace_issues'] = whitespace_issues
    
    # Empty strings
    empty_strings = {}
    for col in str_cols:
        non_null = df[col].dropna()
        if len(non_null) > 0:
            empty_count = (non_null.str.strip() == '').sum()
            if empty_count > 0:
                empty_strings[col] = empty_count
    report['empty_strings'] = empty_strings
    
    return report


def print_section(title, char='═'):
    """Print a formatted section header."""
    width = 70
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def print_subsection(title):
    """Print a formatted subsection header."""
    print(f"\n  ── {title} {'─' * (60 - len(title))}")


# ──────────────────────────────────────────
# Main profiling
# ──────────────────────────────────────────

def main():
    print("=" * 70)
    print("  DISLOG PFE — DATA PROFILING REPORT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    report_lines = []
    report_lines.append("# Data Profiling Report — Dislog PFE\n")
    report_lines.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report_lines.append("---\n")
    
    all_profiles = {}
    
    # ── Overview ──
    print_section("FILE OVERVIEW")
    report_lines.append("## File Overview\n")
    report_lines.append("| Table | File | Size | Encoding |")
    report_lines.append("|-------|------|------|----------|")
    
    for name, info in CSV_FILES.items():
        fpath = DATA_DIR / info['file']
        if fpath.exists():
            size = fmt_size(fpath.stat().st_size)
            print(f"  {name:15s} | {info['file']:20s} | {size:>10s} | {info['encoding']}")
            report_lines.append(f"| {name} | {info['file']} | {size} | {info['encoding']} |")
        else:
            print(f"  {name:15s} | {info['file']:20s} | FILE NOT FOUND!")
            report_lines.append(f"| {name} | {info['file']} | ❌ NOT FOUND | {info['encoding']} |")
    
    report_lines.append("")
    
    # ── Profile each table ──
    for name, info in CSV_FILES.items():
        fpath = DATA_DIR / info['file']
        if not fpath.exists():
            continue
        
        print_section(f"TABLE: {name}")
        report_lines.append(f"\n---\n\n## {name}\n")
        
        try:
            # Special handling for large files
            file_size = fpath.stat().st_size
            
            if file_size > 100_000_000:  # > 100MB — read in chunks
                print(f"  ⚠ Large file ({fmt_size(file_size)}) — using chunked reading...")
                
                # Read first chunk for profiling
                chunks = pd.read_csv(
                    fpath, sep=SEP, encoding=info['encoding'],
                    chunksize=100_000, low_memory=False
                )
                
                first_chunk = next(chunks)
                total_rows = len(first_chunk)
                
                # Count remaining rows
                for chunk in chunks:
                    total_rows += len(chunk)
                
                print(f"  Total rows: {total_rows:,}")
                
                # Profile the first 100k rows
                profile = profile_dataframe(first_chunk, name)
                profile['total_rows'] = total_rows
                profile['sampled'] = True
                
            else:
                # Special handling for Invoice.csv (comma decimal separator)
                if name == "Invoice":
                    df = pd.read_csv(
                        fpath, sep=SEP, encoding=info['encoding'],
                        decimal=',', low_memory=False
                    )
                else:
                    df = pd.read_csv(
                        fpath, sep=SEP, encoding=info['encoding'],
                        low_memory=False
                    )
                
                print(f"  Total rows: {len(df):,}")
                profile = profile_dataframe(df, name)
                profile['total_rows'] = len(df)
                profile['sampled'] = False
            
            all_profiles[name] = profile
            
            # ── Shape ──
            print_subsection("Shape")
            rows = profile['total_rows']
            cols = profile['shape'][1]
            sampled_note = " (profiled on first 100K rows)" if profile.get('sampled') else ""
            print(f"  Rows: {rows:,}{sampled_note}")
            print(f"  Columns: {cols}")
            report_lines.append(f"**Rows**: {rows:,}{sampled_note}  ")
            report_lines.append(f"**Columns**: {cols}\n")
            
            # ── Columns & Types ──
            print_subsection("Columns & Types")
            report_lines.append("### Columns\n")
            report_lines.append("| Column | Dtype | Unique | Missing | Missing % |")
            report_lines.append("|--------|-------|--------|---------|-----------|")
            
            for col in profile['columns']:
                dtype = str(profile['dtypes'][col])
                nunique = profile['nunique'][col]
                miss_count = profile['missing'].loc[col, 'Missing Count']
                miss_pct = profile['missing'].loc[col, 'Missing %']
                
                miss_flag = " ⚠" if miss_count > 0 else ""
                print(f"  {col:30s} | {dtype:10s} | {nunique:>8,} unique | {miss_count:>8,} missing ({miss_pct:.1f}%){miss_flag}")
                report_lines.append(f"| `{col}` | {dtype} | {nunique:,} | {miss_count:,} | {miss_pct:.1f}% |")
            
            report_lines.append("")
            
            # ── Duplicates ──
            print_subsection("Duplicates")
            dup = profile['duplicate_rows']
            dup_pct = (dup / profile['total_rows'] * 100) if profile['total_rows'] > 0 else 0
            if dup > 0:
                print(f"  ⚠ {dup:,} duplicate rows ({dup_pct:.2f}%)")
                report_lines.append(f"⚠ **{dup:,} duplicate rows** ({dup_pct:.2f}%)\n")
            else:
                print(f"  ✅ No duplicate rows")
                report_lines.append("✅ No duplicate rows\n")
            
            # ── Numeric Stats ──
            if profile['numeric_stats'] is not None:
                print_subsection("Numeric Statistics")
                print(profile['numeric_stats'].to_string())
                report_lines.append("### Numeric Statistics\n")
                report_lines.append("```")
                report_lines.append(profile['numeric_stats'].to_string())
                report_lines.append("```\n")
            
            # ── Sample Data ──
            print_subsection("Sample Data (first 3 rows)")
            print(profile['head'].to_string())
            report_lines.append("### Sample Data\n")
            report_lines.append("```")
            report_lines.append(profile['head'].to_string())
            report_lines.append("```\n")
            
            # ── Data Quality Issues ──
            issues = []
            if profile['whitespace_issues']:
                for col, count in profile['whitespace_issues'].items():
                    issues.append(f"Column `{col}`: {count:,} values with leading/trailing whitespace")
            if profile['empty_strings']:
                for col, count in profile['empty_strings'].items():
                    issues.append(f"Column `{col}`: {count:,} empty strings")
            
            if issues:
                print_subsection("⚠ Data Quality Issues")
                report_lines.append("### ⚠ Data Quality Issues\n")
                for issue in issues:
                    print(f"  • {issue}")
                    report_lines.append(f"- {issue}")
                report_lines.append("")
            else:
                print_subsection("✅ No quality issues detected")
                report_lines.append("### ✅ No major quality issues detected\n")
            
        except Exception as e:
            print(f"  ❌ ERROR reading {name}: {e}")
            report_lines.append(f"❌ **ERROR**: {e}\n")
    
    # ── Summary ──
    print_section("SUMMARY")
    report_lines.append("\n---\n\n## Summary\n")
    
    print("\n  Key data quality findings:\n")
    report_lines.append("### Key Findings\n")
    
    findings = [
        "All CSV files use semicolon (;) as delimiter",
        "SalesLine.csv and Invoice.csv use ANSI (cp1252) encoding",
        "Invoice.csv uses comma (,) as decimal separator instead of period (.)",
        "SalesHeader saleid is a long numeric string, not a simple integer",
    ]
    
    # Check for specific issues across profiles
    total_data_rows = sum(p['total_rows'] for p in all_profiles.values())
    
    for finding in findings:
        print(f"  • {finding}")
        report_lines.append(f"- {finding}")
    
    print(f"\n  Total data rows across all tables: {total_data_rows:,}")
    report_lines.append(f"\n**Total data rows**: {total_data_rows:,}\n")
    
    # ── Referential Integrity Preview ──
    print_section("REFERENTIAL INTEGRITY CHECKS")
    report_lines.append("## Referential Integrity\n")
    
    # Load small dimension tables for quick FK checks
    try:
        regions = pd.read_csv(DATA_DIR / "Region.csv", sep=SEP)
        sectors = pd.read_csv(DATA_DIR / "Sector.csv", sep=SEP)
        customers = pd.read_csv(DATA_DIR / "Customer.csv", sep=SEP)
        sellers = pd.read_csv(DATA_DIR / "Seller.csv", sep=SEP)
        products = pd.read_csv(DATA_DIR / "Products.csv", sep=SEP)
        
        # Check Customer → Region
        orphan_regions = set(customers['regionid'].dropna()) - set(regions['regionid'])
        if orphan_regions:
            msg = f"⚠ Customer has {len(orphan_regions)} regionid values not in Region table"
            print(f"  {msg}")
            report_lines.append(f"- {msg}")
            print(f"    Examples: {list(orphan_regions)[:5]}")
        else:
            print(f"  ✅ Customer → Region: all regionid values valid")
            report_lines.append("- ✅ Customer → Region: all valid")
        
        # Check Customer → Sector
        orphan_sectors = set(customers['sectorid'].dropna()) - set(sectors['sectorid'])
        if orphan_sectors:
            msg = f"⚠ Customer has {len(orphan_sectors)} sectorid values not in Sector table"
            print(f"  {msg}")
            report_lines.append(f"- {msg}")
            print(f"    Examples: {list(orphan_sectors)[:5]}")
        else:
            print(f"  ✅ Customer → Sector: all sectorid values valid")
            report_lines.append("- ✅ Customer → Sector: all valid")
        
        # SalesHeader FK checks (sample first 100k rows)
        sh_sample = pd.read_csv(DATA_DIR / "SalesHeader.csv", sep=SEP, nrows=100_000)
        
        orphan_accounts = set(sh_sample['accountid'].dropna()) - set(customers['accountid'].astype(str))
        if orphan_accounts:
            msg = f"⚠ SalesHeader has {len(orphan_accounts)} accountid values not in Customer (sample of 100K rows)"
            print(f"  {msg}")
            report_lines.append(f"- {msg}")
            print(f"    Examples: {list(orphan_accounts)[:5]}")
        else:
            print(f"  ✅ SalesHeader → Customer: all accountid values valid (sample 100K)")
            report_lines.append("- ✅ SalesHeader → Customer: all valid (sample)")
        
        orphan_sellers = set(sh_sample['sellerid'].dropna().astype(str)) - set(sellers['sellerid'].astype(str))
        if orphan_sellers:
            msg = f"⚠ SalesHeader has {len(orphan_sellers)} sellerid values not in Seller (sample of 100K rows)"
            print(f"  {msg}")
            report_lines.append(f"- {msg}")
            print(f"    Examples: {list(orphan_sellers)[:5]}")
        else:
            print(f"  ✅ SalesHeader → Seller: all sellerid values valid (sample 100K)")
            report_lines.append("- ✅ SalesHeader → Seller: all valid (sample)")
        
    except Exception as e:
        print(f"  ❌ Error during FK checks: {e}")
        report_lines.append(f"- ❌ Error: {e}")
    
    report_lines.append("")
    
    # ── Date Range Analysis ──
    print_section("DATE RANGE ANALYSIS")
    report_lines.append("## Date Range\n")
    
    try:
        # Sample SalesHeader for dates
        sh = pd.read_csv(DATA_DIR / "SalesHeader.csv", sep=SEP, 
                         usecols=['orderdate', 'delivdate'], nrows=500_000)
        sh['orderdate'] = pd.to_datetime(sh['orderdate'], errors='coerce')
        sh['delivdate'] = pd.to_datetime(sh['delivdate'], errors='coerce')
        
        min_order = sh['orderdate'].min()
        max_order = sh['orderdate'].max()
        min_deliv = sh['delivdate'].min()
        max_deliv = sh['delivdate'].max()
        
        print(f"  Order dates:    {min_order} → {max_order}")
        print(f"  Delivery dates: {min_deliv} → {max_deliv}")
        report_lines.append(f"- **Order dates**: {min_order} → {max_order}")
        report_lines.append(f"- **Delivery dates**: {min_deliv} → {max_deliv}")
        
        # Check for invalid dates
        invalid_orders = sh['orderdate'].isnull().sum()
        invalid_delivs = sh['delivdate'].isnull().sum()
        if invalid_orders > 0:
            print(f"  ⚠ {invalid_orders:,} invalid order dates")
            report_lines.append(f"- ⚠ {invalid_orders:,} invalid order dates")
        if invalid_delivs > 0:
            print(f"  ⚠ {invalid_delivs:,} invalid delivery dates")
            report_lines.append(f"- ⚠ {invalid_delivs:,} invalid delivery dates")
        
        # Delivery before order?
        late = (sh['delivdate'] < sh['orderdate']).sum()
        if late > 0:
            print(f"  ⚠ {late:,} rows where delivery date < order date")
            report_lines.append(f"- ⚠ {late:,} deliveries before order date")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        report_lines.append(f"- ❌ Error: {e}")
    
    report_lines.append("")
    
    # ── Promotion Analysis ──
    print_section("PROMOTION TYPES (SalesLine)")
    report_lines.append("## Promotion Types\n")
    
    try:
        sl_promo = pd.read_csv(DATA_DIR / "SalesLine.csv", sep=SEP, 
                               encoding='cp1252', usecols=['promotype', 'promovalue'],
                               nrows=500_000)
        promo_counts = sl_promo['promotype'].value_counts()
        print(f"  Promotion type distribution (sample 500K rows):")
        report_lines.append("| Promotion Type | Count | % |")
        report_lines.append("|----------------|-------|---|")
        for ptype, count in promo_counts.items():
            pct = count / len(sl_promo) * 100
            print(f"    {ptype:30s} : {count:>8,} ({pct:.1f}%)")
            report_lines.append(f"| {ptype} | {count:,} | {pct:.1f}% |")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        report_lines.append(f"- ❌ Error: {e}")
    
    report_lines.append("")
    
    # ── Payment Methods ──
    print_section("PAYMENT METHODS (Invoice)")
    report_lines.append("## Payment Methods\n")
    
    try:
        inv = pd.read_csv(DATA_DIR / "Invoice.csv", sep=SEP, encoding='cp1252',
                         decimal=',', usecols=['paymentmethod', 'paymentamount'],
                         nrows=500_000)
        pm_counts = inv['paymentmethod'].value_counts()
        pm_avg = inv.groupby('paymentmethod')['paymentamount'].mean()
        print(f"  Payment method distribution (sample 500K rows):")
        report_lines.append("| Method | Count | % | Avg Amount |")
        report_lines.append("|--------|-------|---|------------|")
        for method in pm_counts.index:
            count = pm_counts[method]
            pct = count / len(inv) * 100
            avg = pm_avg.get(method, 0)
            print(f"    {method:5s} : {count:>8,} ({pct:.1f}%) | Avg: {avg:,.2f}")
            report_lines.append(f"| {method} | {count:,} | {pct:.1f}% | {avg:,.2f} |")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        report_lines.append(f"- ❌ Error: {e}")
    
    report_lines.append("")
    
    # ── Negative Amounts Check ──
    print_section("NEGATIVE AMOUNTS CHECK")
    report_lines.append("## Negative Amounts\n")
    
    try:
        sh_amounts = pd.read_csv(DATA_DIR / "SalesHeader.csv", sep=SEP,
                                 usecols=['bruteamount', 'netamount', 'taxamount', 'totalamount'],
                                 nrows=500_000)
        for col in sh_amounts.columns:
            neg_count = (sh_amounts[col] < 0).sum()
            if neg_count > 0:
                print(f"  ⚠ SalesHeader.{col}: {neg_count:,} negative values")
                report_lines.append(f"- ⚠ SalesHeader.`{col}`: {neg_count:,} negative values")
            else:
                print(f"  ✅ SalesHeader.{col}: no negatives")
                report_lines.append(f"- ✅ SalesHeader.`{col}`: no negatives")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        report_lines.append(f"- ❌ Error: {e}")
    
    # ── Save Report ──
    print_section("REPORT SAVED")
    
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\n  📄 Full report saved to: {REPORT_PATH}")
    print(f"  📊 Profiling complete!\n")


if __name__ == "__main__":
    main()
