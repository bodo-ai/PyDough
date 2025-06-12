WITH _t1 AS (
  SELECT
    COUNT() AS n_searches,
    searches.search_engine,
    times.t_name AS tod
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
    AND times.t_start_hour <= CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
  GROUP BY
    searches.search_engine,
    times.t_name
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
