# Duplicate Prevention Implementation

## Summary

Successfully implemented **MERGE upsert logic** with **unique constraints** to prevent duplicate data in all financial tables.

## What Was Changed

### 1. Database Constraints ✅

Added unique constraints to enforce data integrity at the database level:

| Table | Unique Key | Constraint Name |
|-------|-----------|-----------------|
| `daily_prices` | `ticker + trade_date` | `UQ_daily_prices_ticker_date` |
| `income_statement` | `ticker + fiscal_date + field_name + period_type` | `UQ_income_ticker_date_field_period` |
| `balance_sheet` | `ticker + fiscal_date + field_name + period_type` | `UQ_balance_ticker_date_field_period` |
| `cashflow_statement` | `ticker + fiscal_date + field_name + period_type` | `UQ_cashflow_ticker_date_field_period` |

### 2. MERGE Upsert Logic ✅

Updated both insert methods in [access_db.py](file:///c:/Users/andre/OneDrive/Dokument/vscode/Database_builder/Database_building/access_db.py):

#### `insert_financial_data()`
- Uses SQL MERGE statement
- Creates temporary table for each batch
- **ON MATCH**: Updates existing records with new values
- **ON NO MATCH**: Inserts new records
- Prevents duplicates based on: `ticker + fiscal_date + field_name + period_type`

#### `insert_daily_prices()`
- Uses SQL MERGE statement
- Creates temporary table per ticker
- **ON MATCH**: Updates price data (handles corrections)
- **ON NO MATCH**: Inserts new price records
- Prevents duplicates based on: `ticker + trade_date`

## How It Works

### Before (Old Behavior)
```python
# Simple append - creates duplicates!
df.to_sql("daily_prices", engine, if_exists="append")
```

**Problem:** Running ETL twice would create duplicate rows.

### After (New Behavior)
```sql
MERGE financials.daily_prices AS target
USING temp_table AS source
ON target.ticker = source.ticker 
   AND target.trade_date = source.trade_date
WHEN MATCHED THEN
    UPDATE SET close_price = source.close_price, ...
WHEN NOT MATCHED THEN
    INSERT (ticker, trade_date, ...) VALUES (...)
```

**Benefits:**
- ✅ No duplicates ever
- ✅ Updates existing data if source has corrections
- ✅ Database enforces integrity via constraints
- ✅ Safe to run ETL multiple times

## Files Created/Modified

| File | Purpose |
|------|---------|
| [add_unique_constraints.sql](file:///c:/Users/andre/OneDrive/Dokument/vscode/Database_builder/Database_building/add_unique_constraints.sql) | SQL script to add constraints manually |
| [apply_constraints.py](file:///c:/Users/andre/OneDrive/Dokument/vscode/Database_builder/Database_building/apply_constraints.py) | Python script to apply constraints (already run) |
| [access_db.py](file:///c:/Users/andre/OneDrive/Dokument/vscode/Database_builder/Database_building/access_db.py) | Updated with MERGE upsert logic |
| [upsert_helpers.py](file:///c:/Users/andre/OneDrive/Dokument/vscode/Database_builder/Database_building/upsert_helpers.py) | Reference implementation (not used, kept for documentation) |

## Testing

You can now safely run your ETL pipeline multiple times:

```powershell
.\.venv\Scripts\python.exe run_etl.py
```

**What happens:**
1. First run: Inserts all new data
2. Second run: Updates existing data, inserts only new records
3. No duplicates created!

## Verification Query

Check for duplicates (should return 0 rows):

```sql
-- Check daily_prices
SELECT ticker, trade_date, COUNT(*) as cnt
FROM financials.daily_prices
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1;

-- Check income_statement
SELECT ticker, fiscal_date, field_name, period_type, COUNT(*) as cnt
FROM financials.income_statement
GROUP BY ticker, fiscal_date, field_name, period_type
HAVING COUNT(*) > 1;
```

## Performance Notes

- **Temporary tables** are created per ticker/batch for MERGE operations
- Temp tables are automatically cleaned up after each operation
- MERGE is slightly slower than simple INSERT but ensures data quality
- For 503 companies, expect ~10-15 minutes for full ETL run
