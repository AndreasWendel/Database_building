import pandas as pd
import datetime as dt
import logging
import os
from sqlalchemy import create_engine, text
import urllib

logger = logging.getLogger()

class DBAccess():
    def __init__(self, db_server="localhost", db_name="YourDatabase", driver="ODBC Driver 17 for SQL Server", trusted="yes") -> None:
        """
        Initialising connection to SQL Server database using SQLAlchemy.
        """
        params = urllib.parse.quote_plus(
            f"DRIVER={{{driver}}};"
            f"SERVER={db_server};"
            f"DATABASE={db_name};"
            f"Trusted_Connection={trusted};"
        )
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        
        try:
            self.engine = create_engine(connection_string)
            # Test connection
            with self.engine.connect() as conn:
                pass
        except Exception as e:
            logger.exception(e)
            print("Database connection failed")
            raise

    def get_list_of_needed_updates(self):
        """
        Gets the companies from financials.companies and calculates which symbols
        needs to be updated.
        """
        today = dt.datetime.today()
        
        query = "SELECT symbol, next_earnings_date, last_financials_update FROM financials.companies WHERE active = 1"
        
        try:
            df = pd.read_sql_query(query, self.engine)
        except Exception as e:
            logger.error(f"Failed to read companies table: {e}")
            return []

        if df.empty:
            return []

        df["next_earnings_date"] = pd.to_datetime(df["next_earnings_date"])
        df["last_financials_update"] = pd.to_datetime(df["last_financials_update"])
        
        # Logic: Update if today > next_earnings_date + 1 day AND last_update <= next_earnings_date + 1 day
        # Or if last_update is very old (e.g., > 2 years)
        
        # Handle NaT/None
        df["last_financials_update"] = df["last_financials_update"].fillna(pd.Timestamp("1900-01-01"))
        
        update_mask = (
            ((df["next_earnings_date"] + dt.timedelta(days=1) <= today) & 
             (df["last_financials_update"] <= df["next_earnings_date"] + dt.timedelta(days=1))) |
            (df["last_financials_update"] < (today - dt.timedelta(days=730)))
        )
        
        update_list = df.loc[update_mask, "symbol"].tolist()
        
        if not update_list:
            logging.info("Nothing to update")
        
        return update_list      
    
    def check_earnings_last_update(self):
        """
        Checks if we need to update earnings dates.
        """
        query = "SELECT symbol, next_earnings_date, last_financials_update FROM financials.companies WHERE active = 1"
        try:
            df = pd.read_sql_query(query, self.engine)
            df["next_earnings_date"] = pd.to_datetime(df["next_earnings_date"])
            df["last_financials_update"] = pd.to_datetime(df["last_financials_update"])
            
            # If earnings date is in the past compared to last update, we probably need a new earnings date
            mask = df["next_earnings_date"] < df["last_financials_update"]
            return df.loc[mask, "symbol"].tolist()
        except Exception:
            return []

    def update_earnings_date(self, df):
        """
        Update next_earnings_date in financials.companies
        """
        with self.engine.begin() as conn:
            for i, row in df.iterrows():
                if row["Earnings date"]:
                    sql = text("UPDATE financials.companies SET next_earnings_date = :date WHERE symbol = :symbol")
                    conn.execute(sql, {"date": row["Earnings date"], "symbol": row["Symbol"]})

    def update_last_update(self, time, symbol_list):
        """
        Update last_financials_update in financials.companies
        """
        with self.engine.begin() as conn:
            for symbol in symbol_list:
                sql = text("UPDATE financials.companies SET last_financials_update = :time WHERE symbol = :symbol")
                conn.execute(sql, {"time": time, "symbol": symbol})

    def insert_financial_data(self, list_of_df):
        """
        Insert new data into financials tables.
        """
        failed_update = []
        
        for df in list_of_df:
            if df.empty:
                continue
                
            symbol = df["Symbol"].iloc[0]
            statement_type = df["StatementType"].iloc[0]
            
            # Map statement type to table name
            table_map = {
                "Income Statement": "income_statement",
                "Balance Sheet": "balance_sheet",
                "Cash Flow": "cashflow_statement"
            }
            
            table_name = table_map.get(statement_type)
            if not table_name:
                continue

            # Clean up dataframe for insertion
            # We need to ensure columns match the target table schema or use if_exists='append' with care
            # For now, assuming we dump the raw data or need to map it. 
            # Given the user's schema is just 'financials.income_statement', I'll assume it can take the yfinance columns 
            # OR we might need to be more specific. 
            # YFinance returns dates as columns. We need to melt this?
            # Usually financial tables are (Symbol, Date, Metric, Value) or (Symbol, Date, Revenue, NetIncome...)
            # The previous code did `df.T` so dates became the index.
            
            # Let's assume the target table expects: Symbol, Date, [Metrics...]
            # The df from API is already transposed: Index=Date, Columns=Metrics + Symbol + StatementType
            
            df_to_insert = df.copy()
            df_to_insert.index.name = "date"
            df_to_insert = df_to_insert.reset_index()
            
            # Drop StatementType as it's implied by table
            df_to_insert = df_to_insert.drop(columns=["StatementType"])
            
            try:
                # We should check if data exists to avoid duplicates if no PK constraint
                # But 'append' is what was requested.
                df_to_insert.to_sql(table_name, self.engine, schema="financials", if_exists="append", index=False)
                logging.info(f"{symbol} {statement_type} updated")
            except Exception as e:
                logger.error(f"Failed to insert {statement_type} for {symbol}: {e}")
                failed_update.append(symbol)
                    
        return list(set(failed_update))
    
    def insert_daily_prices(self, list_of_df):
        for df in list_of_df:
            if df.empty: continue
            try:
                df.to_sql("daily_prices", self.engine, schema="financials", if_exists="append")
            except Exception as e:
                logger.error(f"Failed to insert prices: {e}")

    def close_connection(self):
        self.engine.dispose()
