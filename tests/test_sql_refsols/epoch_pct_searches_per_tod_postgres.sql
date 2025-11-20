WITH _t0 AS (
  SELECT
    times.t_name,
    MAX(times.t_start_hour) AS anything_tstarthour,
    COUNT(*) AS n_rows
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  t_name AS tod,
  ROUND(CAST((
    100.0 * n_rows
  ) / SUM(n_rows) OVER () AS DECIMAL), 2) AS pct_searches
FROM _t0
ORDER BY
  anything_tstarthour NULLS FIRST
