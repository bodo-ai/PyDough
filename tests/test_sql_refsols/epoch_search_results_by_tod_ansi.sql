WITH _s3 AS (
  SELECT
    AVG(searches.search_num_results) AS agg_0,
    COUNT() AS agg_1,
    times.t_name AS name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM searches.search_ts)
    AND times.t_start_hour <= EXTRACT(HOUR FROM searches.search_ts)
  GROUP BY
    times.t_name
), _t0 AS (
  SELECT
    times.t_name AS tod,
    ROUND((
      100.0 * COALESCE(_s3.agg_1, 0)
    ) / SUM(COALESCE(_s3.agg_1, 0)) OVER (), 2) AS pct_searches,
    ROUND(_s3.agg_0, 2) AS avg_results,
    times.t_start_hour AS start_hour
  FROM times AS times
  LEFT JOIN _s3 AS _s3
    ON _s3.name = times.t_name
)
SELECT
  tod,
  pct_searches,
  avg_results
FROM _t0
ORDER BY
  start_hour
