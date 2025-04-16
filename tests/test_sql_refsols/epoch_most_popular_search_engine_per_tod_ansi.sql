WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    searches.search_engine,
    times.t_name AS tod
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM searches.search_ts)
    AND times.t_start_hour <= EXTRACT(HOUR FROM searches.search_ts)
  GROUP BY
    searches.search_engine,
    times.t_name
), _t0 AS (
  SELECT
    COALESCE(agg_0, 0) AS n_searches,
    search_engine,
    tod
  FROM _t2
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY tod ORDER BY COALESCE(agg_0, 0) DESC NULLS FIRST, search_engine NULLS LAST) = 1
)
SELECT
  tod,
  search_engine,
  n_searches
FROM _t0
ORDER BY
  tod
