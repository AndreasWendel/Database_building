-- Add columns to financials.companies to support the update logic

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[financials].[companies]') AND name = 'last_financials_update')
BEGIN
    ALTER TABLE [financials].[companies] ADD [last_financials_update] DATETIME2 NULL;
END

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[financials].[companies]') AND name = 'next_earnings_date')
BEGIN
    ALTER TABLE [financials].[companies] ADD [next_earnings_date] DATETIME2 NULL;
END

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[financials].[companies]') AND name = 'active')
BEGIN
    ALTER TABLE [financials].[companies] ADD [active] BIT DEFAULT 1;
END

-- Optional: Initialize active to 1 if it was null (for existing rows)
UPDATE [financials].[companies] SET [active] = 1 WHERE [active] IS NULL;
