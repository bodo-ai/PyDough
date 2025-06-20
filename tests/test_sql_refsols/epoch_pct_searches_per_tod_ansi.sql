WITH _t1 AS (
  SELECT
    COUNT(*) AS agg_0,
    ANY_VALUE(_s0.t_name) AS agg_2,
    ANY_VALUE(_s0.t_start_hour) AS agg_3
  FROM times AS _s0
  JOIN searches AS _s1
    ON _s0.t_end_hour > EXTRACT(HOUR FROM _s1.search_ts)
    AND _s0.t_start_hour <= EXTRACT(HOUR FROM _s1.search_ts)
  GROUP BY
    _s0.t_name
), _t0 AS (
  SELECT
    ROUND((
      100.0 * agg_0
    ) / SUM(agg_0) OVER (), 2) AS pct_searches,
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
