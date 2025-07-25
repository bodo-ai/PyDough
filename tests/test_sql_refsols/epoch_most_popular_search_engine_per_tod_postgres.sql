WITH _t AS (
  SELECT
    COUNT(*) AS n_searches,
    searches.search_engine,
    times.t_name,
    ROW_NUMBER() OVER (PARTITION BY times.t_name ORDER BY COUNT(*) DESC, searches.search_engine) AS _w
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    searches.search_engine,
    times.t_name
)
SELECT
  t_name AS tod,
  search_engine,
  n_searches
FROM _t
WHERE
  _w = 1
ORDER BY
  t_name NULLS FIRST
