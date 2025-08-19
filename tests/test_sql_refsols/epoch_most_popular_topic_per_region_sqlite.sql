WITH _t AS (
  SELECT
    COUNT(DISTINCT searches.search_id) AS n_searches,
    events.ev_typ,
    users.user_region,
    ROW_NUMBER() OVER (PARTITION BY users.user_region ORDER BY COUNT(DISTINCT searches.search_id) DESC) AS _w
  FROM events AS events
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  GROUP BY
    users.user_region,
    2
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  n_searches
FROM _t
WHERE
  _w = 1
