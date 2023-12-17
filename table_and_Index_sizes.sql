with cte as (
SELECT
s.name  as SchemaName,
t.name as TableName,
SUM (ps.used_page_count) as used_pages_count,
SUM (CASE
            WHEN (i.index_id < 2) THEN (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
            ELSE lob_used_page_count + row_overflow_used_page_count
        END) as pages
,ps.row_count
,min(i.index_id) as index_id
FROM sys.dm_db_partition_stats  AS ps 
JOIN sys.tables AS t ON ps.object_id = t.object_id
JOIN sys.indexes AS i ON i.[object_id] = t.[object_id] AND ps.index_id = i.index_id
JOIN sys.schemas as s on s.schema_id=t.schema_id
GROUP BY t.name,ps.row_count,s.name
)
select
       cte.SchemaName, cte.TableName, cte.row_count,
    cast((cte.pages * 8.)/1024 as decimal(10,3)) as TableSizeInMB, 
    cast(((CASE WHEN cte.used_pages_count > cte.pages 
                THEN cte.used_pages_count - cte.pages
                ELSE 0 
          END) * 8./1024) as decimal(10,3)) as IndexSizeInMB,index_id as HasPK
from cte
order by 3 desc
