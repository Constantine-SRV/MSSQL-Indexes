use admintools
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- tbl.Jobs
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'tbl.Jobs' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
	CREATE TABLE [dbo].[tbl.Jobs](
		[iJobID] [int] IDENTITY(1,1) NOT NULL,
		[vJobName] [varchar](8000) NULL,
	 CONSTRAINT [PK_tbl.Jobs] PRIMARY KEY CLUSTERED 
	(
		[iJobID] ASC
	) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
	        ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
	) ON [PRIMARY]
	PRINT 'Table [tbl.Jobs] created'
END
ELSE
	PRINT 'Table [tbl.Jobs] already exists, skipped'
GO

-- =============================================
-- tbl_IndexStatusNorm
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'tbl_IndexStatusNorm' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
	CREATE TABLE [dbo].[tbl_IndexStatusNorm](
		[iIndexStatusID] [int] IDENTITY(1,1) NOT NULL,
		[iDB_ID] [int] NOT NULL,
		[iIndex_id] [varchar](1000) NOT NULL,
		[iSchema_id] [varchar](1000) NOT NULL,
		[iTable_id] [varchar](1000) NOT NULL,
		[iPages] [int] NOT NULL,
		[dAvg_fragmentation_in_percent] [numeric](5, 2) NOT NULL,
		[vcIndexType] [varchar](100) NOT NULL,
		[bIs_lob] [bit] NULL,
		[dtS] [datetime2](0) NOT NULL,
		[iJobID] [int] NULL,
		[cAct] [char](1) NOT NULL,
		[vcRes] [varchar](8000) NULL,
		[dtF] [datetime2](0) NULL,
	 CONSTRAINT [PK_tbl_IndexStatusNorm] PRIMARY KEY CLUSTERED 
	(
		[iIndexStatusID] ASC
	) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
	        ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
	--,OPTIMIZE_FOR_SEQUENTIAL_KEY = ON  --2019 Version
	) ON [PRIMARY]
	) ON [PRIMARY]
	PRINT 'Table [tbl_IndexStatusNorm] created'
END
ELSE
	PRINT 'Table [tbl_IndexStatusNorm] already exists, skipped'
GO

IF NOT EXISTS (
	SELECT 1 FROM sys.default_constraints
	WHERE name = 'DF_tbl_IndexStatusNorm_dt'
	  AND parent_object_id = OBJECT_ID('dbo.tbl_IndexStatusNorm')
)
BEGIN
	ALTER TABLE [dbo].[tbl_IndexStatusNorm]
		ADD CONSTRAINT [DF_tbl_IndexStatusNorm_dt] DEFAULT (getdate()) FOR [dtS]
	PRINT 'Constraint [DF_tbl_IndexStatusNorm_dt] created'
END
ELSE
	PRINT 'Constraint [DF_tbl_IndexStatusNorm_dt] already exists, skipped'
GO

-- =============================================
-- usp_indexMaintanance
-- =============================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_indexMaintanance]
	 @DB_ID int
    ,@PageCountMin INT = 64
    ,@PageCountMax INT = NULL        -- NULL = no upper page count limit
    ,@maxDurationMin int = 10
    ,@RebuildPercent INT = 30
    ,@ReorganizePercent INT = 15    -- 0 or negative = rebuild only, no reorganize
    ,@MAXDOP INT = 24
    ,@PageCompressionON BIT = 1
    ,@IsOnlineRebuild BIT = 1
    ,@IsVersion2012Plus BIT = 1
    ,@IsEntEdition BIT = 1
    ,@IsUpdateStatisticAfterReorganise bit = 0
    ,@maxTbSizeMb numeric(36,2) = 200
    ,@deletePercentExceedingLimit int = 20

/* call example

declare @db_id int = DB_ID()
exec admintools..[usp_indexMaintanance]
      @DB_ID = @db_id
     ,@PageCountMin  = 64
     ,@PageCountMax  = 2000000000   -- or omit / NULL for no upper limit
     ,@maxDurationMin  = 15
    ,@RebuildPercent  = 30
    ,@ReorganizePercent  = 15       -- 0 or negative = rebuild only
    ,@MAXDOP  = 24
    ,@PageCompressionON  = 1
    ,@IsOnlineRebuild  = 1
    ,@IsVersion2012Plus  = 1
    ,@IsEntEdition  = 1

*/

AS
DECLARE @SQL NVARCHAR(MAX), @JobID int = 0, @msgTxt varchar(8000) = ''
SET NOCOUNT ON

-- JobID
DECLARE @jobName varchar(8000)
SELECT @jobName = PROGRAM_NAME + '|' + SYSTEM_USER
FROM sys.dm_exec_sessions AS s
WHERE session_id = @@SPID

IF NOT EXISTS (SELECT iJobID FROM [admintools].dbo.[tbl.Jobs] WHERE [vJobName] = @jobName)
BEGIN
    INSERT INTO [admintools].dbo.[tbl.Jobs] ([vJobName]) VALUES (@jobName)
    SELECT @jobID = @@IDENTITY
END
ELSE
    SELECT @jobID = iJobID FROM [admintools].dbo.[tbl.Jobs] WHERE [vJobName] = @jobName

-- Delete old records if table exceeds size limit
DECLARE @tbSize NUMERIC(36, 2)
SELECT @tbSize = CAST(ROUND(((SUM(au.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2))
FROM [admintools].sys.schemas s
JOIN [admintools].sys.tables t ON s.schema_id = t.schema_id
JOIN [admintools].sys.partitions p ON t.object_id = p.object_id
JOIN [admintools].sys.allocation_units au ON p.partition_id = au.container_id
WHERE t.name = 'tbl_IndexStatusNorm'
GROUP BY s.name, t.name, t.type_desc

IF @tbSize > @maxTbSizeMb
BEGIN
    DELETE TOP (10) PERCENT FROM [admintools].dbo.tbl_IndexStatusNorm
    SET @msgTxt = '!-----deleted --------- ' + CAST(@@ROWCOUNT AS NVARCHAR) + '  ' + CONVERT(varchar(5), getdate(), 108)
    RAISERROR (@msgTxt, 0, 1) WITH NOWAIT
    ALTER INDEX [PK_tbl_IndexStatusNorm] ON [admintools].[dbo].tbl_IndexStatusNorm
        REBUILD PARTITION = ALL
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF,
              ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
END

-- Fill index table
DROP TABLE IF EXISTS #indSt
CREATE TABLE #indSt (
    indexID int, indexName nvarchar(255), Schemaid int, SchemaName nvarchar(255),
    TableID int, TableName nvarchar(255), avg_fragmentation_in_percent numeric(6,2),
    page_count int, is_lob_legacy bit, is_lob bit, partition_number int,
    ds_Type varchar(50), [ID] [int] IDENTITY(1,1)
)

SELECT @sql =
' use ' + db_name(@DB_ID) + '
SELECT
    i.index_id AS indexID, i.name AS indexName, s2.schema_id, s2.name AS SchemaName,
    o.object_id, o.name AS TableName,
    s.avg_fragmentation_in_percent, s.page_count, lob.is_lob_legacy, lob.is_lob,
    partition_number, ds.type
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) s
JOIN sys.indexes i ON i.[object_id] = s.[object_id] AND i.index_id = s.index_id
LEFT JOIN (
    SELECT
          c.[object_id]
        , index_id = ISNULL(i.index_id, 1)
        , is_lob_legacy = MAX(CASE WHEN c.system_type_id IN (34, 35, 99) THEN 1 END)
        , is_lob = MAX(CASE WHEN c.max_length = -1 THEN 1 END)
    FROM sys.columns c
    LEFT JOIN sys.index_columns i ON c.[object_id] = i.[object_id]
        AND c.column_id = i.column_id AND i.index_id > 0
    WHERE c.system_type_id IN (34, 35, 99)
        OR c.max_length = -1
    GROUP BY c.[object_id], i.index_id
) lob ON lob.[object_id] = i.[object_id] AND lob.index_id = i.index_id
JOIN sys.objects o ON o.[object_id] = i.[object_id]
JOIN sys.schemas s2 ON o.[schema_id] = s2.[schema_id]
JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
WHERE i.[type] IN (1, 2)
    AND i.is_disabled = 0
    AND i.is_hypothetical = 0
    AND s.index_level = 0
    AND s.page_count >= ' + CAST(@PageCountMin AS varchar) + '
    ' + CASE WHEN @PageCountMax IS NOT NULL
             THEN 'AND s.page_count < ' + CAST(@PageCountMax AS varchar) + ' '
             ELSE '' END + '
    AND s.alloc_unit_type_desc = ''IN_ROW_DATA''
    AND o.[type] IN (''U'', ''V'')
ORDER BY s.page_count ASC
'

INSERT INTO #indSt (indexID, indexName, Schemaid, SchemaName, TableID, TableName,
    avg_fragmentation_in_percent, page_count, is_lob_legacy, is_lob, partition_number, ds_Type)
EXEC sp_executesql @sql

SELECT * FROM #indSt

DECLARE
    @indexName nvarchar(255), @SchemaName nvarchar(255), @TableName nvarchar(255),
    @avg_fragmentation_in_percent numeric(6,2),
    @page_count int, @is_lob_legacy bit, @is_lob bit, @partition_number int,
    @ds_Type varchar(50), @ID int = 0, @idMax int, @rebuild bit,
    @iIndexStatusNormID int = 0, @indexID int, @SchemaID int, @TableID int

-- Processing
SELECT @idmax = COUNT(*) FROM #indSt

WHILE EXISTS (SELECT * FROM #indSt WHERE id > @id)
BEGIN
    SELECT TOP 1
        @indexName = indexName, @SchemaName = SchemaName, @TableName = TableName,
        @avg_fragmentation_in_percent = avg_fragmentation_in_percent,
        @page_count = page_count, @is_lob_legacy = is_lob_legacy, @is_lob = is_lob,
        @partition_number = partition_number, @ds_Type = ds_Type, @ID = id,
        @indexID = indexID, @SchemaID = schemaid, @TableID = TableID
    FROM #indSt
    WHERE id > @id

    IF  (@ReorganizePercent > 0  AND @avg_fragmentation_in_percent >  @ReorganizePercent)
     OR (@ReorganizePercent <= 0 AND @avg_fragmentation_in_percent >= @RebuildPercent)
    BEGIN
        SELECT @rebuild = IIF(@ReorganizePercent <= 0 OR @avg_fragmentation_in_percent >= @RebuildPercent, 1, 0)

        INSERT INTO [admintools].[dbo].[tbl_IndexStatusNorm]
               ([iDB_ID], [iIndex_id], [iSchema_id], [iTable_id], [iPages],
                [dAvg_fragmentation_in_percent], [vcIndexType], [bIs_lob],
                [dtS], [iJobID], [cAct], [vcRes])
        VALUES (@DB_ID, @indexID, @SchemaID, @TableID, @page_count,
                @avg_fragmentation_in_percent, @ds_Type, @is_lob,
                getdate(), @JobID, IIF(@rebuild=1,'B','O'), 'start')

        SELECT @iIndexStatusNormID = @@IDENTITY

        SELECT @sql =
            'use ' + QUOTENAME(DB_name(@DB_ID)) + ' ALTER INDEX ' + QUOTENAME(@indexName) +
            ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ' +
            CASE WHEN @rebuild = 1 THEN 'REBUILD' ELSE 'REORGANIZE' END +
            ' PARTITION = ' +
            CASE WHEN @ds_Type != 'PS' THEN 'ALL' ELSE CAST(@partition_number AS NVARCHAR(10)) END +
            ' WITH (' +
            CASE WHEN @avg_fragmentation_in_percent >= @RebuildPercent
                THEN 'SORT_IN_TEMPDB = ON' +
                    CASE WHEN @PageCompressionON = 1 THEN ', DATA_COMPRESSION = PAGE' ELSE '' END +
                    CASE WHEN @MAXDOP > 0 THEN ', MAXDOP = ' + STR(@MAXDOP, 2) ELSE '' END +
                    CASE WHEN @IsEntEdition = 1
                            AND @IsOnlineRebuild = 1
                            AND ISNULL(@is_lob_legacy, 0) = 0
                            AND (ISNULL(@is_lob, 0) = 0 OR (@is_lob = 1 AND @IsVersion2012Plus = 1))
                        THEN ', ONLINE = ON (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = ' +
                             CONVERT(nvarchar, @maxDurationMin) + ' MINUTES, ABORT_AFTER_WAIT = SELF ))'
                        ELSE ''
                    END
                ELSE 'LOB_COMPACTION = ON'
            END + ')'

        RAISERROR (@sql, 0, 1) WITH NOWAIT
        BEGIN TRY
            EXEC sys.sp_executesql @sql
            SET @msgTxt = CONVERT(varchar, @id) + ' from ' + CONVERT(varchar, @idmax) +
                          ' pages:' + CONVERT(varchar, @page_count) + ' ' +
                          CONVERT(varchar, getdate(), 120) + ' OK'
            UPDATE [admintools].[dbo].[tbl_IndexStatusNorm]
                SET dtf = getdate(), [vcRes] = 'OK'
            WHERE iIndexStatusID = @iIndexStatusNormID

            IF @rebuild = 0 AND @IsUpdateStatisticAfterReorganise = 1
            BEGIN
                SELECT @sql = 'use ' + QUOTENAME(DB_name(@DB_ID)) +
                    ' UPDATE STATISTICS ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) +
                    ' with fullscan, maxdop=' + STR(@MAXDOP, 2)
                RAISERROR (@sql, 0, 1) WITH NOWAIT
                EXEC sys.sp_executesql @sql
                UPDATE [admintools].[dbo].[tbl_IndexStatusNorm]
                    SET dtf = getdate(), [vcRes] = 'OK', [cAct] = 'S'
                WHERE iIndexStatusID = @iIndexStatusNormID
            END
        END TRY
        BEGIN CATCH
            SET @msgTxt = CONVERT(varchar, @id) + ' from ' + CONVERT(varchar, @idmax) +
                          ' pages:' + CONVERT(varchar, @page_count) + ' ' +
                          CONVERT(varchar, getdate(), 120) + ' ' + ERROR_MESSAGE()
            UPDATE [admintools].[dbo].[tbl_IndexStatusNorm]
                SET dtf = getdate(), [vcRes] = ERROR_MESSAGE()
            WHERE iIndexStatusID = @iIndexStatusNormID
        END CATCH

        RAISERROR (@msgTxt, 0, 1) WITH NOWAIT
    END
    ELSE
    BEGIN
        -- Log only: fragmentation below both thresholds
        INSERT INTO [admintools].[dbo].[tbl_IndexStatusNorm]
               ([iDB_ID], [iIndex_id], [iSchema_id], [iTable_id], [iPages],
                [dAvg_fragmentation_in_percent], [vcIndexType], [bIs_lob],
                [dtS], [iJobID], [cAct], [vcRes], dtf)
        VALUES (@DB_ID, @indexID, @SchemaID, @TableID, @page_count,
                @avg_fragmentation_in_percent, @ds_Type, @is_lob,
                getdate(), @JobID, 'N', 'OK', getdate())
    END
END
GO
