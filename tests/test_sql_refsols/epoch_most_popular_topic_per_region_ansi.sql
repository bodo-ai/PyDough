WITH _t1 AS (
  SELECT
    events.ev_typ AS event_type,
    COUNT(DISTINCT searches.search_id) AS ndistinct_search_id,
    users.user_region AS region
  FROM events AS events
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  GROUP BY
    events.ev_typ,
    users.user_region
), _t0 AS (
  SELECT
    event_type,
    ndistinct_search_id,
    region
  FROM _t1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY ndistinct_search_id DESC NULLS FIRST) = 1
)
SELECT
  region,
  event_type,
  ndistinct_search_id AS n_searches
FROM _t0
