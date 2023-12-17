



use master

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[tbl.Jobs](
	[iJobID] [int] IDENTITY(1,1) NOT NULL,
	[vJobName] [varchar](8000) NULL,
 CONSTRAINT [PK_tbl.Jobs] PRIMARY KEY CLUSTERED 
(
	[iJobID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO






SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

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
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON 
--,OPTIMIZE_FOR_SEQUENTIAL_KEY = ON  --2019 Version
) ON [PRIMARY]

) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tbl_IndexStatusNorm] ADD  CONSTRAINT [DF_tbl_IndexStatusNorm_dt]  DEFAULT (getdate()) FOR [dtS]
GO




SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[usp_indexMaintanance]
	 @DB_ID int
     ,@PageCountMin INT = 64	
     ,@PageCountMax INT = 60000
     ,@maxDurationMin int =10
    , @RebuildPercent INT =30
    , @ReorganizePercent INT =15
    , @MAXDOP INT = 24
    , @PageCompressionON BIT = 1
    , @IsOnlineRebuild BIT = 1
    , @IsVersion2012Plus BIT = 1
    , @IsEntEdition BIT = 1
	,@IsUpdateStatisticAfterReorganise bit=0
	,@maxTbSizeMb numeric(36,2)=200
	,@deletePercentExceedingLimit int =20

/* call example

declare @db_id int =DB_ID()
exec master..[usp_indexMaintanance]
      @DB_ID=@db_id
     ,@PageCountMin  = 64
     ,@PageCountMax  = 2000000000
     ,@maxDurationMin  =15
    , @RebuildPercent  =5
    , @ReorganizePercent  =5
    , @MAXDOP  = 24
    , @PageCompressionON  = 1
    , @IsOnlineRebuild  = 1
    , @IsVersion2012Plus  = 1
    , @IsEntEdition  = 1

*/

as
declare      @SQL NVARCHAR(MAX)     ,@JobID int=0 ,@msgTxt varchar(8000)=''
SET NOCOUNT ON
--JobID
Declare @jobName varchar(8000)
select @jobName=
PROGRAM_NAME+'|'+SYSTEM_USER
FROM sys.dm_exec_sessions AS s
where session_id =@@SPID

if (not exists (select ijobID from [master].dbo.[tbl.Jobs] where [vJobName] = @jobName))
begin
insert into [master].dbo.[tbl.Jobs] ([vJobName]) Values (@jobName)
select @jobID=@@IDENTITY
end
else
select @jobID=ijobID from [master].dbo.[tbl.Jobs] where [vJobName] = @jobName
--delete old Records
declare @tbSize NUMERIC(36, 2)
                select @tbSize= CAST(ROUND(((SUM(au.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) --AS [Table size (MB)]
                FROM [master].sys.schemas s
                JOIN [master].sys.tables t ON s.schema_id = t.schema_id
                JOIN [master].sys.partitions p ON t.object_id = p.object_id
                JOIN [master].sys.allocation_units au ON p.partition_id = au.container_id
                Where t.name = 'tbl_IndexStatusNorm'
                GROUP BY s.name, t.name, t.type_desc
--delete @deletePercentExceedingLimit old records if table size > than @maxTbSizeMb
                if @tbSize>@maxTbSizeMb
                begin
                               delete top (10) percent from [master].dbo.tbl_IndexStatusNorm
                               set @msgTxt='!-----deleted --------- '+ cast(@@ROWCOUNT as nvarchar)  +'  ' + convert(varchar(5),getdate(),108)
                               raiserror (@msgTxt,0,1) with nowait
                               ALTER INDEX [PK_tbl_WhoIsActive] ON [master].[dbo].tbl_IndexStatusNorm REBUILD PARTITION = ALL WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
                end



--fill index table
drop table if exists #indSt
create table #indSt(indexID int,indexName nvarchar(255),Schemaid int,SchemaName nvarchar(255), TableID int,TableName nvarchar(255), avg_fragmentation_in_percent numeric(6,2),
page_count int,is_lob_legacy bit,is_lob bit, partition_number int, ds_Type varchar(50), [ID] [int] IDENTITY(1,1) )
select @sql=
' use ' + db_name(@DB_ID)+ '
select
i.index_id as indexID, i.name as indexName ,s2.schema_id,s2.name as SchemaName ,o.object_id, o.name  as TableName,
  s.avg_fragmentation_in_percent ,s.page_count, lob.is_lob_legacy, lob.is_lob,partition_number,ds.type
		 
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
        AND s.page_count >= '+ cast(@PageCountMin as varchar) +'
		AND s.page_count < '+ cast(@PageCountMax as varchar) +'
        AND s.alloc_unit_type_desc = ''IN_ROW_DATA''
        AND o.[type] IN (''U'', ''V'')
       -- AND s.avg_fragmentation_in_percent > '+ cast(@ReorganizePercent as varchar) +'
       Order by s.page_count  ASC

'


insert into #indSt (indexID,indexName ,Schemaid,SchemaName , TableID,TableName, 
avg_fragmentation_in_percent ,page_count ,is_lob_legacy ,is_lob, partition_number , ds_Type )
 EXEC sp_executesql @sql

select * from #indSt
declare 
@indexName nvarchar(255),@SchemaName nvarchar(255), @TableName nvarchar(255), @avg_fragmentation_in_percent numeric(6,2),
@page_count int,@is_lob_legacy bit,@is_lob bit, @partition_number int, @ds_Type varchar(50), @ID int=0
,@idMax int,@rebuild bit,@iIndexStatusNormID int=0
,@indexID int ,@SchemaID int,@TableID int

--processing
select @idmax= count(*) from #indSt
while exists (select * from #indSt where id>@id)
begin

select top 1
@indexName =indexName, @SchemaName =SchemaName, @TableName =TableName, @avg_fragmentation_in_percent =avg_fragmentation_in_percent,
@page_count =page_count,@is_lob_legacy =is_lob_legacy,@is_lob =is_lob, @partition_number =partition_number, @ds_Type=ds_Type,  @ID =id
,@indexID =indexID ,@SchemaID =schemaid,@TableID =TableID
from  #indSt
where id>@id
if @avg_fragmentation_in_percent > @ReorganizePercent
begin
	select @rebuild= iif((@avg_fragmentation_in_percent >= @RebuildPercent),1,0)

	INSERT INTO [master].[dbo].[tbl_IndexStatusNorm]
           ([iDB_ID]
           ,[iIndex_id]
           ,[iSchema_id]
           ,[iTable_id]
           ,[iPages]
           ,[dAvg_fragmentation_in_percent]
           ,[vcIndexType]
           ,[bIs_lob]
           ,[dtS]
           ,[iJobID]
           ,[cAct]
           ,[vcRes]
           )
     VALUES
           (@DB_ID
			   ,@indexID
			   ,@SchemaID
			   ,@TableID
			   ,@page_count
			   ,@avg_fragmentation_in_percent
			   ,@ds_Type
			   ,@is_lob
			   ,getdate()
			   ,@JobID
			   ,iif(@rebuild=1,'B','O')
			   ,'start')
	select @iIndexStatusNormID=@@IDENTITY

	select @sql='use '+QUOTENAME(DB_name(@DB_ID))+' ALTER INDEX ' + QUOTENAME(@indexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ' +
			CASE WHEN @rebuild=1
				THEN 'REBUILD'
				ELSE 'REORGANIZE'
			END + ' PARTITION = ' +
			CASE WHEN @ds_Type != 'PS'
				THEN 'ALL'
				ELSE CAST(@partition_number AS NVARCHAR(10))
			END + ' WITH (' + 
			CASE WHEN @avg_fragmentation_in_percent >= @RebuildPercent
				THEN 'SORT_IN_TEMPDB = ON' + 
					CASE WHEN @PageCompressionON = 1
						THEN ', DATA_COMPRESSION = PAGE'
						ELSE ''
					END + 
					CASE WHEN @MAXDOP > 0
						THEN ', MAXDOP = ' + STR(@MAXDOP, 2)
						ELSE ''
					END + 
					CASE WHEN @IsEntEdition = 1
							AND @IsOnlineRebuild = 1 
							AND ISNULL(@is_lob_legacy, 0) = 0
							AND (
									ISNULL(@is_lob, 0) = 0
								OR
									(@is_lob = 1 AND @IsVersion2012Plus = 1)
							)
						THEN ', ONLINE = ON (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = '+convert(nvarchar,@maxDurationMin)+' MINUTES, ABORT_AFTER_WAIT = SELF ))'
						ELSE ''
					END
				ELSE 'LOB_COMPACTION = ON'
			END + ')'

--executing and logging
		RAISERROR (@sql, 0, 1) WITH NOWAIT
		BEGIN TRY
			EXEC sys.sp_executesql @sql
			set @msgTxt = convert(varchar,@id) +' from ' + convert(varchar,@idmax) + ' pages:' + convert(varchar,@page_count) + ' ' +  convert(varchar,getdate(),120)  + ' OK'
			update  [master].[dbo].[tbl_IndexStatusNorm] set dtf=getdate(),[vcRes]='OK' where iIndexStatusID= @iIndexStatusNormID
			if @rebuild=0 and @IsUpdateStatisticAfterReorganise=1
			begin -- was reorganise and will do statistic update
				select @sql='use '+QUOTENAME(DB_name(@DB_ID))+'	UPDATE STATISTICS' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' ' +' with fullscan ,maxdop=' + STR(@MAXDOP, 2)
				RAISERROR (@sql, 0, 1) WITH NOWAIT
				EXEC sys.sp_executesql @sql
				update  [master].[dbo].[tbl_IndexStatusNorm] set dtf=getdate(),[vcRes]='OK',[cAct]='S' where iIndexStatusID= @iIndexStatusNormID
			end
		END TRY
		BEGIN CATCH
			set @msgTxt = convert(varchar,@id) +' from ' + convert(varchar,@idmax) + ' pages:' + convert(varchar,@page_count) + ' ' +  convert(varchar,getdate(),120)  +  ' ' +ERROR_MESSAGE()
			update  [master].[dbo].[tbl_IndexStatusNorm] set dtf=getdate(),[vcRes]=ERROR_MESSAGE() where iIndexStatusID= @iIndexStatusNormID
		END CATCH;
		RAISERROR (@msgTxt, 0, 1) WITH NOWAIT
end
else
begin -- only loging if in executing
			   
	INSERT INTO [master].[dbo].[tbl_IndexStatusNorm]
           ([iDB_ID]
           ,[iIndex_id]
           ,[iSchema_id]
           ,[iTable_id]
           ,[iPages]
           ,[dAvg_fragmentation_in_percent]
           ,[vcIndexType]
           ,[bIs_lob]
           ,[dtS]
           ,[iJobID]
           ,[cAct]
           ,[vcRes]
            ,dtf)
     VALUES
           (@DB_ID
			   ,@indexID
			   ,@SchemaID
			   ,@TableID
			   ,@page_count
			   ,@avg_fragmentation_in_percent
			   ,@ds_Type
			   ,@is_lob
			   ,getdate()
			   ,@JobID
			   ,'N'
			   ,'OK'
			   ,getdate())

end
end
GO


