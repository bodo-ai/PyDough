WITH _t AS (
  SELECT
    COUNT(DISTINCT SEARCHES.search_id) AS n_searches,
    EVENTS.ev_typ,
    USERS.user_region,
    ROW_NUMBER() OVER (PARTITION BY USERS.user_region ORDER BY CASE WHEN COUNT(DISTINCT SEARCHES.search_id) IS NULL THEN 1 ELSE 0 END DESC, COUNT(DISTINCT SEARCHES.search_id) DESC) AS _w
  FROM EVENTS AS EVENTS
  JOIN SEARCHES AS SEARCHES
    ON LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN USERS AS USERS
    ON SEARCHES.search_user_id = USERS.user_id
  GROUP BY
    EVENTS.ev_typ,
    USERS.user_region
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  n_searches
FROM _t
WHERE
  _w = 1
