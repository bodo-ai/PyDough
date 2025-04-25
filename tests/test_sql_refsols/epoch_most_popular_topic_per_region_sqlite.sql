WITH _t2 AS (
  SELECT
    COUNT(DISTINCT searches.search_id) AS agg_0,
    events.ev_typ AS event_type,
    users.user_region AS region
  FROM events AS events
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  LEFT JOIN users AS users
    ON searches.search_user_id = users.user_id
  GROUP BY
    events.ev_typ,
    users.user_region
), _t AS (
  SELECT
    COALESCE(agg_0, 0) AS n_searches,
    event_type,
    region,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY COALESCE(agg_0, 0) DESC) AS _w
  FROM _t2
)
SELECT
  region,
  event_type,
  n_searches
FROM _t
WHERE
  _w = 1
