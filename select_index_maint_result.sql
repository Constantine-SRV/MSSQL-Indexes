
-- in master
Use DBtest
SELECT DB_name(iDB_ID) as DBName, tb.name  as TableName, i.name as IndexName 
,[dAvg_fragmentation_in_percent],[iPages],[cAct],[vcRes],
[dtS],datediff(second,[dtS],[dtF]) as durationSec,[dtF],[iJobID]
,[bIs_lob],[vcIndexType],[iIndex_id],iDB_ID  --,[iIndexStatusID] --,s2.name as SchemaName 
FROM [master].[dbo].[tbl_IndexStatusNorm] as s
JOIN sys.indexes i ON i.[object_id] = s.[iTable_id] AND i.index_id = s.[iIndex_id]
JOIN sys.objects tb ON tb.[object_id] = i.[object_id]
JOIN sys.schemas s2 ON tb.[schema_id] = s2.[schema_id]
where 1=1
--and [cAct] <>'N'  -- N-nothing B-Rebuild O-Reorganize S-stat
--and  i.name='' -- index name
--and tb.name ='' -- Table name
--and datediff(second,[dtS],[dtF]) >60
order by [iIndexStatusID] desc

-- in AdminTools  
Use DBtest
SELECT DB_name(iDB_ID) as DBName, tb.name  as TableName, i.name as IndexName 
,[dAvg_fragmentation_in_percent],[iPages],[cAct],[vcRes],
[dtS],datediff(second,[dtS],[dtF]) as durationSec,[dtF],[iJobID]
,[bIs_lob],[vcIndexType],[iIndex_id],iDB_ID  --,[iIndexStatusID] --,s2.name as SchemaName 
FROM [AdminTools].[dbo].[tbl_IndexStatusNorm] as s
JOIN sys.indexes i ON i.[object_id] = s.[iTable_id] AND i.index_id = s.[iIndex_id]
JOIN sys.objects tb ON tb.[object_id] = i.[object_id]
JOIN sys.schemas s2 ON tb.[schema_id] = s2.[schema_id]
where 1=1
--and [cAct] <>'N'  -- N-nothing B-Rebuild O-Reorganize S-stat
--and  i.name='' -- index name
--and tb.name ='' -- Table name
--and datediff(second,[dtS],[dtF]) >60
order by [iIndexStatusID] desc
