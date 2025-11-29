"""
Apply unique constraints to database tables.
Run this ONCE before using the MERGE upsert logic.
"""
from access_db import DBAccess
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)

def apply_constraints():
    """Apply unique constraints to all financial tables"""
    db = DBAccess()
    
    constraints = [
        {
            "name": "UQ_daily_prices_ticker_date",
            "table": "financials.daily_prices",
            "columns": "(ticker, trade_date)",
            "description": "Daily prices: ticker + trade_date must be unique"
        },
        {
            "name": "UQ_income_ticker_date_field_period",
            "table": "financials.income_statement",
            "columns": "(ticker, fiscal_date, field_name, period_type)",
            "description": "Income statement: ticker + fiscal_date + field_name + period_type must be unique"
        },
        {
            "name": "UQ_balance_ticker_date_field_period",
            "table": "financials.balance_sheet",
            "columns": "(ticker, fiscal_date, field_name, period_type)",
            "description": "Balance sheet: ticker + fiscal_date + field_name + period_type must be unique"
        },
        {
            "name": "UQ_cashflow_ticker_date_field_period",
            "table": "financials.cashflow_statement",
            "columns": "(ticker, fiscal_date, field_name, period_type)",
            "description": "Cashflow statement: ticker + fiscal_date + field_name + period_type must be unique"
        }
    ]
    
    try:
        with db.engine.begin() as conn:
            for constraint in constraints:
                print(f"\n{constraint['description']}")
                
                # Check if constraint exists
                check_sql = text(f"""
                    SELECT COUNT(*) as cnt
                    FROM sys.indexes 
                    WHERE name = :constraint_name 
                    AND object_id = OBJECT_ID(:table_name)
                """)
                
                result = conn.execute(check_sql, {
                    "constraint_name": constraint["name"],
                    "table_name": constraint["table"]
                })
                
                exists = result.fetchone()[0] > 0
                
                if exists:
                    print(f"  ✓ Constraint {constraint['name']} already exists")
                else:
                    # Add constraint
                    add_sql = text(f"""
                        ALTER TABLE {constraint['table']}
                        ADD CONSTRAINT {constraint['name']} 
                        UNIQUE {constraint['columns']}
                    """)
                    
                    conn.execute(add_sql)
                    print(f"  ✓ Added constraint {constraint['name']}")
        
        print("\n✅ All unique constraints have been applied successfully!")
        
    except Exception as e:
        print(f"\n❌ Error applying constraints: {e}")
        raise
    finally:
        db.close_connection()

if __name__ == "__main__":
    apply_constraints()
