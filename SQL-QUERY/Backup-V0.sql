use DislogDWH
GO
--Backup complet de la base
BACKUP DATABASE DislogDWH
TO DISK = 'C:\SQLData\DislogDWH_backup_before_fix.bak'
WITH FORMAT, MEDIANAME = 'DislogDWH_Backup',
NAME = 'Full Backup before CLIENT_UNKNOWN fix';

--Vérifie que le backup est OK
RESTORE VERIFYONLY
FROM DISK = 'C:\SQLData\DislogDWH_backup_before_fix.bak';
