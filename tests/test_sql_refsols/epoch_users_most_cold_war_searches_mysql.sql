WITH _t1 AS (
  SELECT
    ANY_VALUE(searches.search_user_id) AS anything_search_user_id
  FROM searches AS searches
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN eras AS eras
    ON eras.er_end_year > EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
    AND eras.er_name = 'Cold War'
    AND eras.er_start_year <= EXTRACT(YEAR FROM CAST(events.ev_dt AS DATETIME))
  GROUP BY
    searches.search_id
), _s5 AS (
  SELECT
    COUNT(*) AS n_cold_war_searches,
    anything_search_user_id
  FROM _t1
  GROUP BY
    2
)
SELECT
  user_name COLLATE utf8mb4_bin AS user_name,
  _s5.n_cold_war_searches
FROM users AS users
JOIN _s5 AS _s5
  ON _s5.anything_search_user_id = users.user_id
ORDER BY
  2 DESC,
  1
LIMIT 3
