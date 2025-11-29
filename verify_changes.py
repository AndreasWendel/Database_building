import logging
from api import API
from access_db import DBAccess
from sp500_list_getter import GetSp500List
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)

def verify():
    print("Starting verification...")
    
    # 1. Update S&P 500 list
    print("\n--- Verifying S&P 500 List Update ---")
    sp500 = GetSp500List()
    sp500.request_to_pd()
    df_sp500 = sp500.get_df()
    print(f"Fetched {len(df_sp500)} companies.")
    print("Columns:", df_sp500.columns.tolist())
    
    if "etfs" in df_sp500.columns and "etf_weights" in df_sp500.columns:
        print("ETF columns present.")
    else:
        print("ERROR: ETF columns missing!")
        
    # Upsert to DB
    try:
        sp500.update_db()
        print("S&P 500 upsert successful.")
    except Exception as e:
        print(f"ERROR: S&P 500 upsert failed: {e}")

    # 2. Fetch and Insert Financials
    print("\n--- Verifying Financials Fetch and Insert ---")
    api = API()
    api.add_list(["AAPL"]) # Test with one company
    print("Fetching financials for AAPL...")
    api.get_financials()
    data, errors, time = api.get_all()
    
    print(f"Fetched {len(data)} dataframes.")
    if data:
        df = data[0]
        print("First dataframe columns:", df.columns.tolist())
        expected_cols = ["ticker", "fiscal_date", "field_name", "field_value", "unit", "source", "period_type", "created_at", "table_name"]
        if all(col in df.columns for col in expected_cols):
            print("Financials dataframe has correct vertical format.")
        else:
            print(f"ERROR: Financials dataframe missing columns. Found: {df.columns.tolist()}")

        # Insert to DB
        db = DBAccess()
        try:
            failed = db.insert_financial_data(data)
            if not failed:
                print("Financials insertion successful.")
            else:
                print(f"Financials insertion reported failures: {failed}")
        except Exception as e:
            print(f"ERROR: Financials insertion failed: {e}")
        finally:
            db.close_connection()
    else:
        print("ERROR: No financials data fetched.")

if __name__ == "__main__":
    verify()
