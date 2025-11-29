-- Add unique constraints to prevent duplicate data
-- Run this script on your database before running the ETL pipeline

-- =====================================================
-- DAILY PRICES: ticker + trade_date must be unique
-- =====================================================
IF NOT EXISTS (
    SELECT * FROM sys.indexes 
    WHERE name = 'UQ_daily_prices_ticker_date' 
    AND object_id = OBJECT_ID('financials.daily_prices')
)
BEGIN
    ALTER TABLE financials.daily_prices
    ADD CONSTRAINT UQ_daily_prices_ticker_date 
    UNIQUE (ticker, trade_date);
    PRINT 'Added unique constraint: UQ_daily_prices_ticker_date';
END
ELSE
BEGIN
    PRINT 'Constraint UQ_daily_prices_ticker_date already exists';
END
GO

-- =====================================================
-- INCOME STATEMENT: ticker + fiscal_date + field_name + period_type must be unique
-- =====================================================
IF NOT EXISTS (
    SELECT * FROM sys.indexes 
    WHERE name = 'UQ_income_ticker_date_field_period' 
    AND object_id = OBJECT_ID('financials.income_statement')
)
BEGIN
    ALTER TABLE financials.income_statement
    ADD CONSTRAINT UQ_income_ticker_date_field_period
    UNIQUE (ticker, fiscal_date, field_name, period_type);
    PRINT 'Added unique constraint: UQ_income_ticker_date_field_period';
END
ELSE
BEGIN
    PRINT 'Constraint UQ_income_ticker_date_field_period already exists';
END
GO

-- =====================================================
-- BALANCE SHEET: ticker + fiscal_date + field_name + period_type must be unique
-- =====================================================
IF NOT EXISTS (
    SELECT * FROM sys.indexes 
    WHERE name = 'UQ_balance_ticker_date_field_period' 
    AND object_id = OBJECT_ID('financials.balance_sheet')
)
BEGIN
    ALTER TABLE financials.balance_sheet
    ADD CONSTRAINT UQ_balance_ticker_date_field_period
    UNIQUE (ticker, fiscal_date, field_name, period_type);
    PRINT 'Added unique constraint: UQ_balance_ticker_date_field_period';
END
ELSE
BEGIN
    PRINT 'Constraint UQ_balance_ticker_date_field_period already exists';
END
GO

-- =====================================================
-- CASHFLOW STATEMENT: ticker + fiscal_date + field_name + period_type must be unique
-- =====================================================
IF NOT EXISTS (
    SELECT * FROM sys.indexes 
    WHERE name = 'UQ_cashflow_ticker_date_field_period' 
    AND object_id = OBJECT_ID('financials.cashflow_statement')
)
BEGIN
    ALTER TABLE financials.cashflow_statement
    ADD CONSTRAINT UQ_cashflow_ticker_date_field_period
    UNIQUE (ticker, fiscal_date, field_name, period_type);
    PRINT 'Added unique constraint: UQ_cashflow_ticker_date_field_period';
END
ELSE
BEGIN
    PRINT 'Constraint UQ_cashflow_ticker_date_field_period already exists';
END
GO

PRINT 'All unique constraints have been applied successfully!';
