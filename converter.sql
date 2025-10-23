/*
Purpose: Import the CSV file "Cykle - EN-600 EN-500 - Kotrubcik V1.csv" into SQL Server.

What this script does
- Creates a staging table with 8 NVARCHAR columns (safe ASCII names)
- Uses BULK INSERT to load the semicolon-delimited CSV
- Handles SQL Server 2022+ CSV parser when available; otherwise uses classic BULK INSERT
- Trims whitespace and removes fully blank rows after import

How to run
1) Open in SSMS (or sqlcmd) connected to the target database
2) Ensure the SQL Server service account has READ access to the CSV file path
   - Recommended: Move the CSV to something like C:\Data and grant read permissions to the SQL Server service account
3) Update @FilePath as needed (path below points to your current workspace file)
4) Execute the whole script

Notes and pitfalls
- The file appears to use semicolons as delimiters and a European decimal comma
- The header in this file spans multiple physical lines (quoted) and there is a blank row; the script accounts for this
- If you are NOT on SQL Server 2022 or later, quoted newlines are not parsed as a single header row; we skip those physical lines via FIRSTROW
- If you see mojibake (garbled diacritics), try adjusting CODEPAGE between '1250' (Central European) and '65001' (UTF-8)
 - Permissions: Your login needs ADMINISTER BULK OPERATIONS (or be in BULKADMIN/fixed server role) to run BULK INSERT.
*/

SET NOCOUNT ON;

DECLARE @FilePath NVARCHAR(4000) = N'c:\Users\bvontor\Desktop\fucking databaze\Cykle - EN-600 EN-500 - Kotrubcik V1.csv';
DECLARE @MajorVersion INT = TRY_CONVERT(INT, SERVERPROPERTY('ProductMajorVersion'));

-- Target table and station (set these for your environment)
DECLARE @TargetTable SYSNAME = N'dbo.<SET_YOUR_TARGET_TABLE_NAME>'; -- e.g., N'dbo.vyroba_parametry'
DECLARE @PracovisteId BIGINT = 1;                                   -- set your workstation id
DECLARE @OeeDefault FLOAT = 0.85;                                    -- used only if takt_plan is missing

IF OBJECT_ID(N'dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg', N'U') IS NOT NULL
BEGIN
	DROP TABLE dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg;
END

CREATE TABLE dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg
(
	Col1_V1                      NVARCHAR(255) NULL,  -- first column (often empty)
	Pojemnosc                    NVARCHAR(255) NULL,  -- pojemność
	Srednica                     NVARCHAR(255) NULL,  -- średnica
	Grubosc                      NVARCHAR(255) NULL,  -- grubość
	Obrobka_cieplna_Cykl_s       NVARCHAR(255) NULL,  -- "Obróbka cieplna CYKL [s]"
	OEE                          NVARCHAR(255) NULL,  -- OEE
	Uwagi                        NVARCHAR(255) NULL,  -- uwagi
	Extra                        NVARCHAR(255) NULL   -- trailing/extra column
);

-- 2) Import data from CSV
-- Important: SQL Server must be able to read @FilePath from the server's filesystem.
-- If this fails with an OS error, copy the CSV to C:\Data (or similar) and grant read access to the SQL Server service account.

DECLARE @sql NVARCHAR(MAX);

IF (@MajorVersion IS NOT NULL AND @MajorVersion >= 16)
BEGIN
	/* SQL Server 2022+ path: use the CSV parser (handles quoted fields and embedded newlines) */
	SET @sql = N'
BULK INSERT dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg
FROM ''' + @FilePath + N'''
WITH (
	FORMAT = ''CSV'',                -- CSV parser (SQL Server 2022+)
	FIELDTERMINATOR = '';'',
	FIELDQUOTE = '"',               -- honor quoted fields
	ROWTERMINATOR = ''0x0a'',        -- LF; SQL Server handles CRLF automatically
	FIRSTROW = 2,                    -- skip header row (CSV parser treats it as one logical line)
	CODEPAGE = ''1250'',             -- adjust to ''65001'' if your file is UTF-8
	KEEPNULLS,
	TABLOCK
);
';
END
ELSE
BEGIN
	/* Older SQL Server: classic BULK INSERT (does not fully parse CSV quotes)
	   The given file has a multi-line quoted header plus a blank row.
	   We skip the first 4 physical lines so data starts at the first actual data row.
	   If your data appears shifted, adjust FIRSTROW up/down until it aligns. */
	SET @sql = N'
BULK INSERT dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg
FROM ''' + @FilePath + N'''
WITH (
	FIELDTERMINATOR = '';'',
	ROWTERMINATOR = ''0x0a'',        -- LF (works with CRLF files as well)
	FIRSTROW = 5,                    -- skip 3 physical header lines + 1 blank line
	CODEPAGE = ''1250'',             -- adjust to ''65001'' if your file is UTF-8
	KEEPNULLS,
	TABLOCK
);
';
END

PRINT N'Importing from: ' + @FilePath;
BEGIN TRY
	EXEC sys.sp_executesql @sql;
	PRINT N'BULK INSERT finished successfully.';
END TRY
BEGIN CATCH
	DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
	DECLARE @ErrNum INT = ERROR_NUMBER();
	DECLARE @ErrSev INT = ERROR_SEVERITY();
	DECLARE @ErrSta INT = ERROR_STATE();
	DECLARE @ErrLin INT = ERROR_LINE();
	RAISERROR('BULK INSERT failed (%d, Sev %d, State %d, Line %d): %s', 16, 1, @ErrNum, @ErrSev, @ErrSta, @ErrLin, @ErrMsg);
	RETURN;
END CATCH;

-- 3) Post-load cleanup: trim and convert empty strings to NULL
UPDATE s
SET
	Col1_V1                = NULLIF(LTRIM(RTRIM(Col1_V1)), ''),
	Pojemnosc              = NULLIF(LTRIM(RTRIM(Pojemnosc)), ''),
	Srednica               = NULLIF(LTRIM(RTRIM(Srednica)), ''),
	Grubosc                = NULLIF(LTRIM(RTRIM(Grubosc)), ''),
	Obrobka_cieplna_Cykl_s = NULLIF(LTRIM(RTRIM(Obrobka_cieplna_Cykl_s)), ''),
	OEE                    = NULLIF(LTRIM(RTRIM(OEE)), ''),
	Uwagi                  = NULLIF(LTRIM(RTRIM(Uwagi)), ''),
	Extra                  = NULLIF(LTRIM(RTRIM(Extra)), '')
FROM dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg AS s;

-- Remove rows that are completely blank
DELETE s
FROM dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg AS s
WHERE
	s.Col1_V1 IS NULL AND s.Pojemnosc IS NULL AND s.Srednica IS NULL AND s.Grubosc IS NULL AND
	s.Obrobka_cieplna_Cykl_s IS NULL AND s.OEE IS NULL AND s.Uwagi IS NULL AND s.Extra IS NULL;

-- 4) Optional: inspect a sample
SELECT TOP (20) *
FROM dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg;

-- Row count summary
SELECT COUNT(*) AS RowCount_Staging
FROM dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg;

-- 6) Insert into destination table (set @TargetTable and @PracovisteId above)
IF @TargetTable IS NULL OR @TargetTable LIKE N'%<SET_YOUR_TARGET_TABLE_NAME>%'
BEGIN
	PRINT N'NOTE: @TargetTable is not set. Set it to your existing table (schema.name) and rerun the INSERT section.';
END
ELSE
BEGIN
	DECLARE @Schema SYSNAME = ISNULL(PARSENAME(@TargetTable, 2), N'dbo');
	DECLARE @Table  SYSNAME = PARSENAME(@TargetTable, 1);

	DECLARE @InsertSql NVARCHAR(MAX) = N'
INSERT INTO ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N' (
	pracoviste_id, prumer, stena, litry, zavit, takt, takt_plan
)
SELECT
	@PracovisteId AS pracoviste_id,
	/* prumer from Srednica: average if range (e.g., 140-141) */
	CAST(
		CASE WHEN CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) > 0 THEN
			(
				TRY_CONVERT(float, LEFT(REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.''), CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) - 1)) +
				TRY_CONVERT(float, STUFF(REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.''), 1, CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')), ''''))
			) / 2.0
		ELSE TRY_CONVERT(float, REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) END AS float
	) AS prumer,
	/* stena from Grubosc: average if range (e.g., 2,8-2,85) */
	CAST(
		CASE WHEN CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) > 0 THEN
			(
				TRY_CONVERT(float, LEFT(REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.''), CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) - 1)) +
				TRY_CONVERT(float, STUFF(REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.''), 1, CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')), ''''))
			) / 2.0
		ELSE TRY_CONVERT(float, REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) END AS float
	) AS stena,
	/* litry from Pojemnosc: extract number or average of range; remove ''elip'' suffix */
	CAST(
		CASE WHEN CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) > 0 THEN
			(
				TRY_CONVERT(float, LEFT(REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.''), CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) - 1)) +
				TRY_CONVERT(float, STUFF(REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.''), 1, CHARINDEX(''-'', REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')), ''''))
			) / 2.0
		ELSE TRY_CONVERT(float, REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) END AS float
	) AS litry,
	/* zavit: no explicit column in CSV -> use Uwagi as a free-text placeholder */
	NULLIF(s.Uwagi, '''') AS zavit,
	/* takt from Obrobka_cieplna_Cykl_s */
	TRY_CONVERT(float, REPLACE(REPLACE(NULLIF(s.Obrobka_cieplna_Cykl_s, ''''), '','', ''.''), '' '', '''')) AS takt,
	/* takt_plan: prefer column 6 if numeric; else compute from takt / @OeeDefault */
	COALESCE(
		TRY_CONVERT(float, REPLACE(REPLACE(NULLIF(s.OEE, ''''), '','', ''.''), '' '', '''')),
		CASE WHEN TRY_CONVERT(float, REPLACE(REPLACE(NULLIF(s.Obrobka_cieplna_Cykl_s, ''''), '','', ''.''), '' '', '''')) IS NOT NULL
			 THEN CAST(ROUND(TRY_CONVERT(float, REPLACE(REPLACE(NULLIF(s.Obrobka_cieplna_Cykl_s, ''''), '','', ''.''), '' '', '''')) / @OeeDefault, 2) AS float)
		END
	) AS takt_plan
FROM dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg AS s
WHERE
	-- Skip rows with no meaningful numeric content at all
	(
		TRY_CONVERT(float, REPLACE(REPLACE(REPLACE(NULLIF(s.Srednica, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) IS NOT NULL OR
		TRY_CONVERT(float, REPLACE(REPLACE(REPLACE(NULLIF(s.Grubosc,  ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) IS NOT NULL OR
		TRY_CONVERT(float, REPLACE(REPLACE(REPLACE(NULLIF(s.Pojemnosc, ''''), ''elip'', ''''), '' '', ''''), '','', ''.'')) IS NOT NULL OR
		TRY_CONVERT(float, REPLACE(REPLACE(NULLIF(s.Obrobka_cieplna_Cykl_s, ''''), '','', ''.''), '' '', '''')) IS NOT NULL OR
		TRY_CONVERT(float, REPLACE(REPLACE(NULLIF(s.OEE, ''''), '','', ''.''), '' '', '''')) IS NOT NULL
	);
';

	BEGIN TRY
		EXEC sys.sp_executesql @InsertSql, N'@PracovisteId BIGINT, @OeeDefault FLOAT', @PracovisteId=@PracovisteId, @OeeDefault=@OeeDefault;
		PRINT N'Insert into target table completed.';
	END TRY
	BEGIN CATCH
		DECLARE @e NVARCHAR(4000) = ERROR_MESSAGE();
		RAISERROR(N'Insert into %s failed: %s', 16, 1, @TargetTable, @e);
	END CATCH;

	-- Report how many rows now exist in target for this pracoviste_id (if column exists)
	DECLARE @CountSql NVARCHAR(MAX) = N'SELECT COUNT(*) AS InsertedForPracoviste FROM ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N' WHERE pracoviste_id = @pid';
	BEGIN TRY
		EXEC sys.sp_executesql @CountSql, N'@pid BIGINT', @pid=@PracovisteId;
	END TRY
	BEGIN CATCH
		PRINT N'Count summary skipped (target table does not have pracoviste_id or query failed).';
	END CATCH;
END

/*
5) Optional: move from staging to a typed table

CREATE TABLE dbo.Cykle_EN600_EN500_Kotrubcik_V1
(
	-- Example typed schema (adjust as needed)
	Col1_V1                      NVARCHAR(255) NULL,
	Pojemnosc                    NVARCHAR(255) NULL,
	Srednica                     NVARCHAR(255) NULL,
	Grubosc                      NVARCHAR(255) NULL,
	Obrobka_cieplna_Cykl_s       NVARCHAR(255) NULL,
	OEE                          DECIMAL(9,2)  NULL, -- if you want to store as number, see REPLACE comma below
	Uwagi                        NVARCHAR(255) NULL,
	Extra                        NVARCHAR(255) NULL
);

INSERT INTO dbo.Cykle_EN600_EN500_Kotrubcik_V1
(
	Col1_V1, Pojemnosc, Srednica, Grubosc, Obrobka_cieplna_Cykl_s,
	OEE, Uwagi, Extra
)
SELECT
	Col1_V1,
	Pojemnosc,
	Srednica,
	Grubosc,
	Obrobka_cieplna_Cykl_s,
	TRY_CONVERT(DECIMAL(9,2), REPLACE(OEE, ',', '.')) AS OEE, -- convert decimal commas to dot
	Uwagi,
	Extra
FROM dbo.Cykle_EN600_EN500_Kotrubcik_V1_stg;
*/

-- Done
PRINT N'Import complete.';
