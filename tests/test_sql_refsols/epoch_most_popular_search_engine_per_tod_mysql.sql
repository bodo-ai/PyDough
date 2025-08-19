WITH _t AS (
  SELECT
    COUNT(*) AS n_searches,
    SEARCHES.search_engine,
    TIMES.t_name,
    ROW_NUMBER() OVER (PARTITION BY TIMES.t_name ORDER BY CASE WHEN COUNT(*) IS NULL THEN 1 ELSE 0 END DESC, COUNT(*) DESC, CASE WHEN SEARCHES.search_engine COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, SEARCHES.search_engine COLLATE utf8mb4_bin) AS _w
  FROM TIMES AS TIMES
  JOIN SEARCHES AS SEARCHES
    ON TIMES.t_end_hour > HOUR(SEARCHES.search_ts)
    AND TIMES.t_start_hour <= HOUR(SEARCHES.search_ts)
  GROUP BY
    SEARCHES.search_engine,
    TIMES.t_name
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
