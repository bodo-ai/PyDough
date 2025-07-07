WITH _t1 AS (
  SELECT
    COUNT(*) AS n_searches,
    searches.search_engine,
    times.t_name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(searches.search_ts AS DATETIME))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(searches.search_ts AS DATETIME))
  GROUP BY
    searches.search_engine,
    times.t_name
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY times.t_name ORDER BY COUNT(*) DESC NULLS FIRST, searches.search_engine NULLS LAST) = 1
)
SELECT
  t_name AS tod,
  search_engine,
  n_searches
FROM _t1
ORDER BY
  t_name
