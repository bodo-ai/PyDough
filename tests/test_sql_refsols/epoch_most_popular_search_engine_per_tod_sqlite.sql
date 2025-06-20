WITH _t1 AS (
  SELECT
    COUNT(*) AS n_searches,
    _s1.search_engine,
    _s0.t_name AS tod
  FROM times AS _s0
  JOIN searches AS _s1
    ON _s0.t_end_hour > CAST(STRFTIME('%H', _s1.search_ts) AS INTEGER)
    AND _s0.t_start_hour <= CAST(STRFTIME('%H', _s1.search_ts) AS INTEGER)
  GROUP BY
    _s1.search_engine,
    _s0.t_name
), _t AS (
  SELECT
    n_searches,
    search_engine,
    tod,
    ROW_NUMBER() OVER (PARTITION BY tod ORDER BY n_searches DESC, search_engine) AS _w
  FROM _t1
)
SELECT
  tod,
  search_engine,
  n_searches
FROM _t
WHERE
  _w = 1
ORDER BY
  tod
