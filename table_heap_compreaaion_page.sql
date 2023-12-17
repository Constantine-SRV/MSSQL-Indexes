declare @rowsMin bigint=0
declare @rowsMax bigint=CAST(0x7FFFFFFFFFFFFFFF AS bigint)
declare @alwaysRebuildHeap bit=1

DECLARE @Table_catalog NVARCHAR(128)
DECLARE @Table_schema NVARCHAR(128)
DECLARE @Table_name NVARCHAR(128)
DECLARE @Data_compression_desc NVARCHAR(128)
DECLARE @index_id NVARCHAR(128)
DECLARE @cmd NVARCHAR(4000)
DECLARE @msgTxt VARCHAR(8000)
Declare @step int=0, @count varchar(10)=0

--ALTER TABLE [db1].[dbo].[tbl1] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE, MAXDOP = 16, online=on (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = 10 minutes, ABORT_AFTER_WAIT = SELF)))
set nocount on
drop table if exists #t1

SELECT DISTINCT Table_catalog, Table_schema, Table_name, sp.data_compression_desc,sp.index_id,rows
into #t1
FROM INFORMATION_SCHEMA.TABLES
inner JOIN sys.partitions AS sp ON sp.object_id = OBJECT_ID(Table_catalog + '.' + Table_schema + '.' + Table_name)
WHERE TABLE_TYPE = 'BASE TABLE' and sp.index_id<2
and rows> @rowsMin
and rows <=@rowsMax
ORDER BY Table_catalog, Table_schema, Table_name asc

select * from #t1
select @count= convert(varchar,count(*)) from #t1
while exists( select * from #t1)
BEGIN
	select top 1 @Table_catalog=Table_catalog, @Table_schema=Table_schema, @Table_name=Table_name, @Data_compression_desc=data_compression_desc,@index_id=index_id
	from #t1
	set @step=@step+1
	BEGIN TRY
		set @msgTxt= convert (varchar,getdate(),120)  + ' step:'+convert(varchar,@step)+' from:'+@count +' tbl:' +@Table_name +' compression:' +@Data_compression_desc + ' Ind:' +@index_id
		if @Data_compression_desc not in ('ROW','NONE') and (@index_id>0 or @alwaysRebuildHeap=0)
		begin 
			set @msgTxt=@msgTxt+' No Action'
			RAISERROR (@msgTxt, 0, 1) WITH NOWAIT
		end Else
		begin
			SET @cmd = 'ALTER TABLE [' + @Table_catalog + '].[' + @Table_schema + '].[' + @Table_name + '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE, MAXDOP = 16 , online=on (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = 10 minutes, ABORT_AFTER_WAIT = SELF)))'
			set @msgTxt=@msgTxt +'
	'+@cmd
			RAISERROR (@msgTxt, 0, 1) WITH NOWAIT
			EXEC (@cmd) 
		end
	END TRY
	BEGIN CATCH
	set @msgTxt=ERROR_MESSAGE()
		RAISERROR (@msgTxt, 0, 1) WITH NOWAIT
	END CATCH;
	delete from #t1 where  @Table_catalog=Table_catalog and @Table_schema=Table_schema and  @Table_name=Table_name 
END;
