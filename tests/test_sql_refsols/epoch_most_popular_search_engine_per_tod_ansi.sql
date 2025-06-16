WITH _t1 AS (
  SELECT
    COUNT() AS n_searches,
    times.t_name AS tod,
    searches.search_engine
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > EXTRACT(HOUR FROM searches.search_ts)
    AND times.t_start_hour <= EXTRACT(HOUR FROM searches.search_ts)
  GROUP BY
    times.t_name,
    searches.search_engine
), _t0 AS (
  SELECT
    n_searches,
    tod,
    search_engine
  FROM _t1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY tod ORDER BY n_searches DESC NULLS FIRST, search_engine NULLS LAST) = 1
)
SELECT
  tod,
  search_engine,
  n_searches
FROM _t0
ORDER BY
  tod
