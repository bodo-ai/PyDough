WITH _t0 AS (
  SELECT
    ANY_VALUE(TIMES.t_name) AS anything_t_name,
    ANY_VALUE(TIMES.t_start_hour) AS anything_t_start_hour,
    COUNT(*) AS n_rows
  FROM TIMES AS TIMES
  JOIN SEARCHES AS SEARCHES
    ON TIMES.t_end_hour > HOUR(SEARCHES.search_ts)
    AND TIMES.t_start_hour <= HOUR(SEARCHES.search_ts)
  GROUP BY
    TIMES.t_name
)
SELECT
  anything_t_name AS tod,
  ROUND((
    100.0 * n_rows
  ) / SUM(n_rows) OVER (), 2) AS pct_searches
FROM _t0
ORDER BY
  anything_t_start_hour
