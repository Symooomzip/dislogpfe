USE DislogDWH
SELECT TOP 20 AccountName FROM DimCustomer WHERE AccountName LIKE 'CLIENT_%';
-- Should show CLIENT_ACC00123 style entries instead of 'Unknown'

SELECT COUNT(*) FROM DimCustomer WHERE AccountName LIKE 'CLIENT_%';



-- Check the actual fix worked
SELECT AccountID, AccountName 
FROM DimCustomer 
WHERE AccountName LIKE 'CLIENT_%'

-- Correct count check
SELECT COUNT(*) FROM DimCustomer 
WHERE LEFT(AccountName, 7) = 'CLIENT_'
-- Should be ~10,341