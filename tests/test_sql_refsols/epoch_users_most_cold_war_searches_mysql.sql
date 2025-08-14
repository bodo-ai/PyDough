WITH _t0 AS (
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
    COUNT(*) AS n_cold_war_searches,
    anything_search_user_id
  FROM _t0
  GROUP BY
    anything_search_user_id
)
SELECT
  USERS.user_name,
  _s5.n_cold_war_searches
FROM USERS AS USERS
JOIN _s5 AS _s5
  ON USERS.user_id = _s5.anything_search_user_id
ORDER BY
  n_cold_war_searches DESC,
  user_name
LIMIT 3
