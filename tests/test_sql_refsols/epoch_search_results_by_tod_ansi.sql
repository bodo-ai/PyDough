WITH _t1 AS (
  SELECT
    AVG(searches.search_num_results) AS agg_0,
    COUNT(*) AS agg_1,
    ANY_VALUE(times.t_name) AS agg_3,
    ANY_VALUE(times.t_start_hour) AS agg_4
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM searches.search_ts)
    AND times.t_start_hour <= EXTRACT(HOUR FROM searches.search_ts)
  GROUP BY
    times.t_name
), _t0 AS (
  SELECT
    ROUND(agg_0, 2) AS avg_results,
    ROUND((
      100.0 * agg_1
    ) / SUM(agg_1) OVER (), 2) AS pct_searches,
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
