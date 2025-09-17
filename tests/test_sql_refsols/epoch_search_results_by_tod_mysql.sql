WITH _t0 AS (
  SELECT
    TIMES.t_name,
    ANY_VALUE(TIMES.t_start_hour) AS anything_t_start_hour,
    AVG(SEARCHES.search_num_results) AS avg_search_num_results,
    COUNT(*) AS n_rows
  FROM TIMES AS TIMES
  JOIN SEARCHES AS SEARCHES
    ON TIMES.t_end_hour > HOUR(SEARCHES.search_ts)
    AND TIMES.t_start_hour <= HOUR(SEARCHES.search_ts)
  GROUP BY
    1
)
SELECT
  t_name AS tod,
  ROUND((
    100.0 * n_rows
  ) / SUM(n_rows) OVER (), 2) AS pct_searches,
  ROUND(avg_search_num_results, 2) AS avg_results
FROM _t0
ORDER BY
  anything_t_start_hour
