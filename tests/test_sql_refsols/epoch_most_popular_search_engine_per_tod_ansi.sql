WITH _t1 AS (
  SELECT
    COUNT(*) AS n_searches,
    _s1.search_engine,
    _s0.t_name AS tod
  FROM times AS _s0
  JOIN searches AS _s1
    ON _s0.t_end_hour > EXTRACT(HOUR FROM _s1.search_ts)
    AND _s0.t_start_hour <= EXTRACT(HOUR FROM _s1.search_ts)
  GROUP BY
    _s1.search_engine,
    _s0.t_name
), _t0 AS (
  SELECT
    n_searches,
    search_engine,
    tod
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
