WITH _t0 AS (
  SELECT
    times.t_name,
    ANY_VALUE(times.t_start_hour) AS anything_t_start_hour,
    COUNT(*) AS n_rows
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > HOUR(CAST(searches.search_ts AS TIMESTAMP))
    AND times.t_start_hour <= HOUR(CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  t_name AS tod,
  ROUND((
    100.0 * n_rows
  ) / SUM(n_rows) OVER (), 2) AS pct_searches
FROM _t0
ORDER BY
  anything_t_start_hour NULLS FIRST
