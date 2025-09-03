WITH _t1 AS (
  SELECT
    COUNT(DISTINCT searches.search_id) AS n_searches,
    events.ev_typ,
    users.user_region
  FROM events AS events
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  GROUP BY
    2,
    3
), _t AS (
  SELECT
    n_searches,
    ev_typ,
    user_region,
    ROW_NUMBER() OVER (PARTITION BY user_region ORDER BY n_searches DESC) AS _w
  FROM _t1
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  n_searches
FROM _t
WHERE
  _w = 1
