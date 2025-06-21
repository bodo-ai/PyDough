WITH _T1 AS (
  SELECT
    COUNT(*) AS AGG_0,
    ANY_VALUE(TIMES.t_name) AS AGG_2,
    ANY_VALUE(TIMES.t_start_hour) AS AGG_3
  FROM TIMES AS TIMES
  JOIN SEARCHES AS SEARCHES
    ON TIMES.t_end_hour > HOUR(SEARCHES.search_ts)
    AND TIMES.t_start_hour <= HOUR(SEARCHES.search_ts)
  GROUP BY
    TIMES.t_name
), _T0 AS (
  SELECT
    ROUND((
      100.0 * AGG_0
    ) / SUM(AGG_0) OVER (), 2) AS PCT_SEARCHES,
    AGG_2 AS TOD,
    AGG_3
  FROM _T1
)
SELECT
  TOD AS tod,
  PCT_SEARCHES AS pct_searches
FROM _T0
ORDER BY
  AGG_3 NULLS FIRST
