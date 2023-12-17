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
        AND s.page_count >= 0
		AND s.page_count >0
        AND s.alloc_unit_type_desc = 'IN_ROW_DATA'
        AND o.[type] IN ('U', 'V')
   Order by s.page_count  ASC
