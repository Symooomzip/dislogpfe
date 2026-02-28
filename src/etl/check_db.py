import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from src.config import get_sqlserver_connection_string

def main():
    try:
        engine = create_engine(get_sqlserver_connection_string())
        tables = [
            "DimDate", 
            "DimCustomer", 
            "DimSeller", 
            "DimProduct", 
            "DimPromotion", 
            "DimPaymentMethod", 
            "FactSales", 
            "FactInvoices"
        ]
        
        print("Checking row counts in the Data Warehouse...")
        for table in tables:
            try:
                query = f"SELECT COUNT(*) as cnt FROM {table}"
                df = pd.read_sql(query, engine)
                count = df.iloc[0]['cnt']
                print(f"✅ {table}: {count:,} rows")
            except Exception as e:
                print(f"❌ {table}: Error checking table - {e}")
                
    except Exception as e:
        print(f"Failed to connect to database: {e}")

if __name__ == "__main__":
    main()
