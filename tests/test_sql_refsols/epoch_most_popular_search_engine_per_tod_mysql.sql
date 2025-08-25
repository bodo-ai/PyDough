WITH _t2 AS (
  SELECT
    COUNT(*) AS n_searches,
    searches.search_engine,
    times.t_name
  FROM times AS times
  JOIN searches AS searches
    ON times.t_end_hour > HOUR(searches.search_ts)
    AND times.t_start_hour <= HOUR(searches.search_ts)
  GROUP BY
    2,
    3
), _t AS (
  SELECT
    n_searches,
    search_engine,
    t_name,
    ROW_NUMBER() OVER (PARTITION BY t_name ORDER BY CASE WHEN n_searches IS NULL THEN 1 ELSE 0 END DESC, n_searches DESC, CASE WHEN search_engine COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, search_engine COLLATE utf8mb4_bin) AS _w
  FROM _t2
)
SELECT
  t_name COLLATE utf8mb4_bin AS tod,
  search_engine,
  n_searches
FROM _t
WHERE
  _w = 1
ORDER BY
  1
