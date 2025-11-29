"""
Helper functions for upsert operations (insert or update)
"""
from sqlalchemy import text
import pandas as pd
import logging

logger = logging.getLogger()

def upsert_daily_prices(engine, df, ticker):
    """
    Upsert daily prices using MERGE statement.
    Updates existing records, inserts new ones.
    """
    if df.empty:
        return
    
    # Create a temporary table
    temp_table = f"#temp_prices_{ticker.replace('-', '_')}"
    
    try:
        with engine.begin() as conn:
            # Insert data into temp table
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
            logger.info(f"Upserted prices for {ticker}: {result.rowcount} rows affected")
            
    except Exception as e:
        logger.error(f"Failed to upsert prices for {ticker}: {e}")
        raise


def upsert_financial_data(engine, df, table_name, ticker):
    """
    Upsert financial statement data using MERGE.
    """
    if df.empty:
        return
    
    temp_table = f"#temp_financials_{ticker.replace('-', '_')}"
    
    try:
        with engine.begin() as conn:
            # Insert data into temp table
            df_to_insert = df.drop(columns=['table_name'])
            df_to_insert.to_sql(temp_table, conn, if_exists='replace', index=False)
            
            # MERGE statement
            merge_sql = text(f"""
                MERGE financials.{table_name} AS target
                USING {temp_table} AS source
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
            logger.info(f"Upserted {table_name} for {ticker}: {result.rowcount} rows affected")
            
    except Exception as e:
        logger.error(f"Failed to upsert {table_name} for {ticker}: {e}")
        raise
