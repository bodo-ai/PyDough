WITH _t1 AS (
  SELECT
    COUNT(DISTINCT SEARCHES.search_id) AS n_searches,
    EVENTS.ev_typ,
    USERS.user_region
  FROM EVENTS AS EVENTS
  JOIN SEARCHES AS SEARCHES
    ON LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN USERS AS USERS
    ON SEARCHES.search_user_id = USERS.user_id
  GROUP BY
    2,
    3
), _t AS (
  SELECT
    n_searches,
    ev_typ,
    user_region,
    ROW_NUMBER() OVER (PARTITION BY user_region ORDER BY CASE WHEN n_searches IS NULL THEN 1 ELSE 0 END DESC, n_searches DESC) AS _w
  FROM _t1
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  n_searches
FROM _t
WHERE
  _w = 1
