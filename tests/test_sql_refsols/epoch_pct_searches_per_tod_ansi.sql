WITH _t1 AS (
  SELECT
    ANY_VALUE(times.t_name) AS anything_t_name,
    ANY_VALUE(times.t_start_hour) AS anything_t_start_hour,
    COUNT(*) AS n_rows
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(searches.search_ts AS DATETIME))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(searches.search_ts AS DATETIME))
  GROUP BY
    times.t_name
), _t0 AS (
  SELECT
    ROUND((
      100.0 * n_rows
    ) / SUM(n_rows) OVER (), 2) AS pct_searches,
    anything_t_name,
    anything_t_start_hour
  FROM _t1
)
SELECT
  anything_t_name AS tod,
  pct_searches
FROM _t0
ORDER BY
  anything_t_start_hour
