WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    MAX(times.t_name) AS agg_2,
    MAX(times.t_start_hour) AS agg_3
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
    AND times.t_start_hour <= CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
  GROUP BY
    times.t_name
), _t0 AS (
  SELECT
    ROUND(CAST((
      100.0 * agg_0
    ) AS REAL) / SUM(agg_0) OVER (), 2) AS pct_searches,
    agg_2 AS tod,
    agg_3
  FROM _t1
)
SELECT
  tod,
  pct_searches
FROM _t0
ORDER BY
  agg_3
