DECLARE @DB_ID   INT = DB_ID()   -- или нужную базу
DECLARE @PageCountMin INT = 64
DECLARE @N       INT = 3         -- количество заданий

;WITH IndexPages AS (
    SELECT
        s.page_count,
        SUM(s.page_count) OVER ()                                          AS total_pages,
        SUM(s.page_count) OVER (ORDER BY s.page_count ROWS UNBOUNDED PRECEDING) AS cumulative_pages
    FROM sys.dm_db_index_physical_stats(@DB_ID, NULL, NULL, NULL, NULL) s
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.type IN (1, 2)
      AND i.is_disabled = 0
      AND i.is_hypothetical = 0
      AND s.index_level = 0
      AND s.alloc_unit_type_desc = 'IN_ROW_DATA'
      AND s.page_count >= @PageCountMin
),
Grouped AS (
    SELECT
        page_count,
        total_pages,
        CEILING(cumulative_pages * 1.0 / (total_pages * 1.0 / @N)) AS grp
    FROM IndexPages
)
SELECT
    grp                    AS [Job#],
    MIN(page_count)        AS PageCountMin,
    MAX(page_count)        AS PageCountMax,
    SUM(page_count)        AS TotalPages,
    COUNT(*)               AS IndexCount
FROM Grouped
GROUP BY grp
ORDER BY grp
