WITH _t2 AS (
  SELECT
    COUNT(*) AS n_searches,
    searches.search_engine,
    times.t_name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
    AND times.t_start_hour <= CAST(STRFTIME('%H', searches.search_ts) AS INTEGER)
  GROUP BY
    2,
    3
), _t AS (
  SELECT
    n_searches,
    search_engine,
    t_name,
    ROW_NUMBER() OVER (PARTITION BY t_name ORDER BY n_searches DESC, search_engine) AS _w
  FROM _t2
)
SELECT
  t_name AS tod,
  search_engine,
  n_searches
FROM _t
WHERE
  _w = 1
ORDER BY
  1
