WITH _t0 AS (
  SELECT
    MAX(times.t_start_hour) AS anything_t_start_hour,
    AVG(CAST(searches.search_num_results AS DECIMAL)) AS avg_search_num_results,
    COUNT(*) AS n_rows,
    times.t_name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
    AND times.t_start_hour <= EXTRACT(HOUR FROM CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    4
)
SELECT
  t_name AS tod,
  ROUND(CAST((
    100.0 * n_rows
  ) / SUM(n_rows) OVER () AS DECIMAL), 2) AS pct_searches,
  ROUND(CAST(avg_search_num_results AS DECIMAL), 2) AS avg_results
FROM _t0
ORDER BY
  anything_t_start_hour NULLS FIRST
