WITH _t1 AS (
  SELECT
    AVG(searches.search_num_results) AS agg_0,
    COUNT(*) AS agg_1,
    MAX(times.t_name) AS agg_3,
    MAX(times.t_start_hour) AS agg_4
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
    AND times.t_start_hour <= CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
  GROUP BY
    times.t_name
), _t0 AS (
  SELECT
    ROUND(agg_0, 2) AS avg_results,
    ROUND(CAST((
      100.0 * agg_1
    ) AS REAL) / SUM(agg_1) OVER (), 2) AS pct_searches,
    agg_3 AS tod,
    agg_4
  FROM _t1
)
SELECT
  tod,
  pct_searches,
  avg_results
FROM _t0
ORDER BY
  agg_4
