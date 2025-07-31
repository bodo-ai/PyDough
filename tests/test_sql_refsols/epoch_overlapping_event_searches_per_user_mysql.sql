WITH _s0 AS (
  SELECT
    user_id,
    user_name
  FROM USERS
), _t1 AS (
  SELECT
    ANY_VALUE(_s0.user_id) AS anything_user_id,
    ANY_VALUE(_s0.user_name) AS anything_user_name
  FROM _s0 AS _s0
  JOIN SEARCHES AS SEARCHES
    ON SEARCHES.search_user_id = _s0.user_id
  JOIN EVENTS AS EVENTS
    ON LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN SEARCHES AS SEARCHES_2
    ON LOWER(SEARCHES_2.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN _s0 AS _s7
    ON SEARCHES_2.search_user_id = _s7.user_id AND _s0.user_name <> _s7.user_name
  GROUP BY
    SEARCHES.search_id,
    _s0.user_id
)
SELECT
  ANY_VALUE(anything_user_name) AS user_name,
  COUNT(*) AS n_searches
FROM _t1
GROUP BY
  anything_user_id
ORDER BY
  n_searches DESC,
  ANY_VALUE(anything_user_name)
LIMIT 4
