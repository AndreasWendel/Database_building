import pandas as pd
import datetime as dt
import logging
import os
from sqlalchemy import create_engine, text
import urllib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger()

class DBAccess():
    def __init__(self, db_server=None, db_name=None, driver=None, trusted=None) -> None:
        """
        Initialising connection to SQL Server database using SQLAlchemy.
        Loads configuration from .env file if parameters not provided.
        """
        # Load from environment variables if not provided
        db_server = db_server or os.getenv("DB_SERVER", "localhost")
        db_name = db_name or os.getenv("DB_NAME", "YourDatabase")
        driver = driver or os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
        trusted = trusted or os.getenv("DB_TRUSTED", "yes")
        
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
        
        query = "SELECT ticker, next_earnings_date, last_financials_update FROM financials.companies WHERE active = 1"
        
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
        
        update_list = df.loc[update_mask, "ticker"].tolist()
        
        if not update_list:
            logging.info("Nothing to update")
        
        return update_list      
    
    def check_earnings_last_update(self):
        """
        Checks if we need to update earnings dates.
        """
        query = "SELECT ticker, next_earnings_date, last_financials_update FROM financials.companies WHERE active = 1"
        try:
            df = pd.read_sql_query(query, self.engine)
            df["next_earnings_date"] = pd.to_datetime(df["next_earnings_date"])
            df["last_financials_update"] = pd.to_datetime(df["last_financials_update"])
            
            # If earnings date is in the past compared to last update, we probably need a new earnings date
            mask = df["next_earnings_date"] < df["last_financials_update"]
            return df.loc[mask, "ticker"].tolist()
        except Exception:
            return []

    def get_all_active_companies(self):
        """
        Gets all active companies from financials.companies.
        Used for daily price updates.
        """
        query = "SELECT ticker FROM financials.companies WHERE active = 1"
        try:
            df = pd.read_sql_query(query, self.engine)
            return df["ticker"].tolist()
        except Exception as e:
            logger.error(f"Failed to read active companies: {e}")
            return []

    def update_earnings_date(self, df):
        """
        Update next_earnings_date in financials.companies
        """
        with self.engine.begin() as conn:
            for i, row in df.iterrows():
                if row["Earnings date"]:
                    sql = text("UPDATE financials.companies SET next_earnings_date = :date WHERE ticker = :ticker")
                    conn.execute(sql, {"date": row["Earnings date"], "ticker": row["Symbol"]})

    def update_last_update(self, time, symbol_list):
        """
        Update last_financials_update in financials.companies
        """
        with self.engine.begin() as conn:
            for symbol in symbol_list:
                sql = text("UPDATE financials.companies SET last_financials_update = :time WHERE ticker = :ticker")
                conn.execute(sql, {"time": time, "ticker": symbol})

    def insert_financial_data(self, list_of_df):
        """
        Upsert financial data using SQL MERGE.
        Inserts new records or updates existing ones based on unique key.
        Expects DataFrames in vertical format with 'table_name' column.
        """
        failed_update = []
        
        for df in list_of_df:
            if df.empty:
                continue
            
            if "table_name" not in df.columns:
                logging.error("DataFrame missing 'table_name' column")
                continue
            
            table_name = df["table_name"].iloc[0]
            ticker = df["ticker"].iloc[0] if "ticker" in df.columns else "unknown"
            
            # Create temp table name (safe for SQL)
            temp_table = f"#temp_{table_name}_{ticker.replace('-', '_')}"
            
            try:
                with self.engine.begin() as conn:
                    # Drop table_name column before inserting to temp table
                    df_to_merge = df.drop(columns=["table_name"])
                    
                    # Insert data into temporary table
                    df_to_merge.to_sql(temp_table.replace('#', ''), conn, if_exists='replace', index=False)
                    
                    # MERGE statement
                    merge_sql = text(f"""
                        MERGE financials.{table_name} AS target
                        USING {temp_table.replace('#', '')} AS source
                        ON target.ticker = source.ticker 
                           AND target.fiscal_date = source.fiscal_date
                           AND target.field_name = source.field_name
                           AND target.period_type = source.period_type
                        WHEN MATCHED THEN
                            UPDATE SET
                                field_value = source.field_value,
                                unit = source.unit,
                                source = source.source,
                                created_at = source.created_at
                        WHEN NOT MATCHED THEN
                            INSERT (ticker, fiscal_date, field_name, field_value, unit, source, period_type, created_at)
                            VALUES (source.ticker, source.fiscal_date, source.field_name, source.field_value,
                                    source.unit, source.source, source.period_type, source.created_at);
                    """)
                    
                    result = conn.execute(merge_sql)
                    logging.info(f"Upserted {table_name} for {ticker}: {result.rowcount} rows affected")
                    
            except Exception as e:
                logger.error(f"Failed to upsert {table_name} for {ticker}: {e}")
                failed_update.append(ticker)
        
        return list(set(failed_update))

    def upsert_companies(self, df):
        """
        Upsert companies into financials.companies.
        Updates etfs and etf_weights if present.
        """
        if df.empty:
            return

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Check if exists
                check_sql = text("SELECT ticker FROM financials.companies WHERE ticker = :ticker")
                result = conn.execute(check_sql, {"ticker": row["ticker"]}).fetchone()
                
                if result:
                    # Update
                    update_sql = text("""
                        UPDATE financials.companies 
                        SET name = :name, 
                            sector = :sector, 
                            industry = :industry,
                            etfs = :etfs,
                            etf_weights = :etf_weights
                        WHERE ticker = :ticker
                    """)
                    conn.execute(update_sql, {
                        "name": row["name"],
                        "sector": row["sector"],
                        "industry": row["industry"],
                        "etfs": row.get("etfs"),
                        "etf_weights": row.get("etf_weights"),
                        "ticker": row["ticker"]
                    })
                else:
                    # Insert
                    insert_sql = text("""
                        INSERT INTO financials.companies (ticker, name, sector, industry, etfs, etf_weights)
                        VALUES (:ticker, :name, :sector, :industry, :etfs, :etf_weights)
                    """)
                    conn.execute(insert_sql, {
                        "ticker": row["ticker"],
                        "name": row["name"],
                        "sector": row["sector"],
                        "industry": row["industry"],
                        "etfs": row.get("etfs"),
                        "etf_weights": row.get("etf_weights")
                    })
    
    def insert_daily_prices(self, list_of_df):
        """
        Upsert daily price data using SQL MERGE.
        Inserts new records or updates existing ones based on ticker + trade_date.
        """
        for df in list_of_df:
            if df.empty:
                continue
            
            ticker = df['ticker'].iloc[0] if 'ticker' in df.columns else 'unknown'
            
            # Create temp table name (safe for SQL)
            temp_table = f"temp_prices_{ticker.replace('-', '_')}"
            
            try:
                with self.engine.begin() as conn:
                    # Insert data into temporary table
                    df.to_sql(temp_table, conn, if_exists='replace', index=False)
                    
                    # MERGE statement
                    merge_sql = text(f"""
                        MERGE financials.daily_prices AS target
                        USING {temp_table} AS source
                        ON target.ticker = source.ticker 
                           AND target.trade_date = source.trade_date
                        WHEN MATCHED THEN
                            UPDATE SET
                                open_price = source.open_price,
                                close_price = source.close_price,
                                high_price = source.high_price,
                                low_price = source.low_price,
                                volume = source.volume,
                                created_at = source.created_at
                        WHEN NOT MATCHED THEN
                            INSERT (ticker, trade_date, open_price, close_price, high_price, low_price, volume, created_at)
                            VALUES (source.ticker, source.trade_date, source.open_price, source.close_price, 
                                    source.high_price, source.low_price, source.volume, source.created_at);
                    """)
                    
                    result = conn.execute(merge_sql)
                    logging.info(f"Upserted prices for {ticker}: {result.rowcount} rows affected")
                    
                    # Clean up temp table
                    conn.execute(text(f"DROP TABLE {temp_table}"))
                    
            except Exception as e:
                logger.error(f"Failed to upsert prices for {ticker}: {e}")

    def close_connection(self):
        self.engine.dispose()
