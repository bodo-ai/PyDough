WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    times.t_name AS name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
    AND times.t_start_hour <= CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
  GROUP BY
    times.t_name
), _t0 AS (
  SELECT
    ROUND(
      CAST((
        100.0 * COALESCE(_s3.agg_0, 0)
      ) AS REAL) / SUM(COALESCE(_s3.agg_0, 0)) OVER (),
      2
    ) AS pct_searches,
    times.t_name AS tod,
    times.t_start_hour AS start_hour
  FROM times AS times
  LEFT JOIN _s3 AS _s3
    ON _s3.name = times.t_name
)
SELECT
  tod,
  pct_searches
FROM _t0
ORDER BY
  start_hour
