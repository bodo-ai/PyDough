WITH _t0 AS (
  SELECT
    events.ev_typ,
    users.user_region,
    COUNT(DISTINCT searches.search_id) AS ndistinct_search_id
  FROM events AS events
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  GROUP BY
    1,
    2
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_region ORDER BY COUNT(DISTINCT searches.search_id) DESC NULLS FIRST) = 1
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  ndistinct_search_id AS n_searches
FROM _t0
