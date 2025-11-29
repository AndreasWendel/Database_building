import yfinance as yf
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

symbol = "AAPL"
ticker = yf.Ticker(symbol)

print("Fetching income statement...")
try:
    df = ticker.income_stmt
    print("Income Statement empty?", df.empty)
    if not df.empty:
        print("Columns:", df.columns)
        print("Index:", df.index)
        print("Head:", df.head())
        
        # Test melt logic
        df = df.reset_index()
        df = df.rename(columns={"index": "field_name"})
        df_melted = df.melt(id_vars=["field_name"], var_name="fiscal_date", value_name="field_value")
        print("Melted head:", df_melted.head())
except Exception as e:
    print(f"Error: {e}")
