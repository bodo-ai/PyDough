WITH _T1 AS (
  SELECT
    AVG(SEARCHES.search_num_results) AS AGG_0,
    COUNT(*) AS AGG_1,
    ANY_VALUE(TIMES.t_name) AS AGG_3,
    ANY_VALUE(TIMES.t_start_hour) AS AGG_4
  FROM TIMES AS TIMES
  JOIN SEARCHES AS SEARCHES
    ON TIMES.t_end_hour > DATE_PART(HOUR, SEARCHES.search_ts)
    AND TIMES.t_start_hour <= DATE_PART(HOUR, SEARCHES.search_ts)
  GROUP BY
    TIMES.t_name
), _T0 AS (
  SELECT
    ROUND(AGG_0, 2) AS AVG_RESULTS,
    ROUND((
      100.0 * AGG_1
    ) / SUM(AGG_1) OVER (), 2) AS PCT_SEARCHES,
    AGG_3 AS TOD,
    AGG_4
  FROM _T1
)
SELECT
  TOD AS tod,
  PCT_SEARCHES AS pct_searches,
  AVG_RESULTS AS avg_results
FROM _T0
ORDER BY
  AGG_4 NULLS FIRST
