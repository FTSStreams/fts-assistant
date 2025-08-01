-- Create monthly_totals table for storing historical monthly wager data
-- Run this in DBeaver or your PostgreSQL client

-- First, check if the table already exists and create it if not
CREATE TABLE IF NOT EXISTS monthly_totals (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_wager DECIMAL(15,2) DEFAULT 0.00,
    total_weighted_wager DECIMAL(15,2) DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (year, month)
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_monthly_totals_date ON monthly_totals (year, month);
CREATE INDEX IF NOT EXISTS idx_monthly_totals_created_at ON monthly_totals (created_at);

-- If migrating from the old settings table format, uncomment and run this:
/*
-- Migrate existing data from settings table (if any exists)
INSERT INTO monthly_totals (year, month, total_wager, total_weighted_wager, created_at)
SELECT 
    CAST(split_part(split_part(key, '_', 3), '_', 1) AS INTEGER) as year,
    CAST(split_part(key, '_', 4) AS INTEGER) as month,
    CAST(split_part(value, ',', 1) AS DECIMAL(15,2)) as total_wager,
    CAST(split_part(value, ',', 2) AS DECIMAL(15,2)) as total_weighted_wager,
    NOW() as created_at
FROM settings 
WHERE key LIKE 'monthly_totals_%'
ON CONFLICT (year, month) DO NOTHING;
*/

-- Verify the table structure
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'monthly_totals' 
ORDER BY ordinal_position;
