WITH _t1 AS (
  SELECT
    ANY_VALUE(SEARCHES.search_user_id) AS anything_search_user_id
  FROM SEARCHES AS SEARCHES
  JOIN EVENTS AS EVENTS
    ON LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN ERAS AS ERAS
    ON ERAS.er_end_year > EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
    AND ERAS.er_name = 'Cold War'
    AND ERAS.er_start_year <= EXTRACT(YEAR FROM CAST(EVENTS.ev_dt AS DATETIME))
  GROUP BY
    SEARCHES.search_id
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows,
    anything_search_user_id
  FROM _t1
  GROUP BY
    2
)
SELECT
  user_name COLLATE utf8mb4_bin AS user_name,
  _s5.n_rows AS n_cold_war_searches
FROM USERS AS USERS
JOIN _s5 AS _s5
  ON USERS.user_id = _s5.anything_search_user_id
ORDER BY
  2 DESC,
  1
LIMIT 3
