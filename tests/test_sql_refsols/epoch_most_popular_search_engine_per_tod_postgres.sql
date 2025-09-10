WITH _t2 AS (
  SELECT
    searches.search_engine,
    times.t_name,
    COUNT(*) AS n_rows
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    1,
    2
), _t AS (
  SELECT
    search_engine,
    t_name,
    n_rows,
    ROW_NUMBER() OVER (PARTITION BY t_name ORDER BY n_rows DESC, search_engine) AS _w
  FROM _t2
)
SELECT
  t_name AS tod,
  search_engine,
  n_rows AS n_searches
FROM _t
WHERE
  _w = 1
ORDER BY
  1 NULLS FIRST
