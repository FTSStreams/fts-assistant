-- Add total_wager column to existing monthly_totals table
-- Run this in DBeaver

-- Add the missing total_wager column
ALTER TABLE monthly_totals 
ADD COLUMN total_wager DECIMAL(15,2) DEFAULT 0.00;

-- Verify the updated table structure
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'monthly_totals' 
ORDER BY ordinal_position;

-- Check current data
SELECT * FROM monthly_totals ORDER BY year, month;
