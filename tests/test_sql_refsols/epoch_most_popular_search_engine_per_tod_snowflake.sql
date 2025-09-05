WITH _t1 AS (
  SELECT
    COUNT(*) AS n_rows,
    searches.search_engine,
    times.t_name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > HOUR(CAST(searches.search_ts AS TIMESTAMP))
    AND times.t_start_hour <= HOUR(CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    2,
    3
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY t_name ORDER BY COUNT(*) DESC, searches.search_engine) = 1
)
SELECT
  t_name AS tod,
  search_engine,
  n_rows AS n_searches
FROM _t1
ORDER BY
  1 NULLS FIRST
