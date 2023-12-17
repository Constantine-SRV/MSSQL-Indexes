SELECT sp.stats_id, 
       tb.name as tbName,
          stat.name as indName, 
       filter_definition, 
       last_updated, 
       rows, 
       rows_sampled, 
       steps, 
       unfiltered_rows, 
       modification_counter
          ,modification_counter*100.0/rows as percentOfUpdatetRows
          --,*
FROM sys.stats AS stat
JOIN sys.objects tb ON tb.[object_id] = stat.[object_id]
     CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
       where stat.name not like '_WA%' 
       -- and  last_updated <dateadd(day,-5,getdate())
       --and modification_counter>0
       --and rows >50000
       and tb.type='U'-- user tables
       order by 
        --rows desc
       modification_counter*100.0/rows desc
       , sp.last_updated desc
