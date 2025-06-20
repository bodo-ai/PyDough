WITH _t1 AS (
  SELECT
    AVG(_s1.search_num_results) AS agg_0,
    COUNT(*) AS agg_1,
    ANY_VALUE(_s0.t_name) AS agg_3,
    ANY_VALUE(_s0.t_start_hour) AS agg_4
  FROM times AS _s0
  JOIN searches AS _s1
    ON _s0.t_end_hour > EXTRACT(HOUR FROM _s1.search_ts)
    AND _s0.t_start_hour <= EXTRACT(HOUR FROM _s1.search_ts)
  GROUP BY
    _s0.t_name
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
