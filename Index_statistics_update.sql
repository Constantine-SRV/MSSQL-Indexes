drop table if exists #t1
select distinct tb.name as tableName,modification_counter*100.0/rows  as mc,rows as tblRows
into #t1
FROM sys.stats AS stat
JOIN sys.objects tb ON tb.[object_id] = stat.[object_id]
     CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
       where stat.name not like '_WA%' 
       -- and  last_updated <dateadd(day,-5,getdate())
       --and modification_counter>0
	   and modification_counter*100.0/rows >0.5
       and rows >10
       and tb.type='U'-- user tables


select * from #t1 order by mc desc

declare @tblName nvarchar(500),@sql Nvarchar(4000),@msgTxt nvarchar(4000),@rows int, @mc numeric(7,2)

while exists (select top 1 * from #t1)
begin
	select top 1 @tblName=TableName,@rows=tblRows, @mc=mc from #t1 order by mc desc

	set @sql=N'Update STATISTICS ' + @tblName+' with fullscan, maxdop=16'
	set @msgTxt =convert(varchar,getdate(),120)  + ' ' + @sql + ' rows:' + convert(varchar,@rows) + ' mod:' + convert(varchar,@mc) + '%% left:' + convert(varchar,(select count(*) from #t1))
	raiserror (@msgTxt,0,1) with nowait
	begin try
		execute sp_executesql @sql
	end try
	begin catch
		set @msgTxt =@tblName + ' ' +ERROR_MESSAGE()
		raiserror (@msgTxt,0,1) with nowait
	end catch
	delete from #t1 where tableName=@tblName
end

--DBCC FREEPROCCACHE WITH NO_INFOMSGS;
