WITH _t1 AS (
  SELECT
    events.ev_typ,
    users.user_region,
    COUNT(DISTINCT searches.search_id) AS ndistinct_searchid
  FROM events AS events
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  GROUP BY
    1,
    2
), _t AS (
  SELECT
    ev_typ,
    user_region,
    ndistinct_searchid,
    ROW_NUMBER() OVER (PARTITION BY user_region ORDER BY ndistinct_searchid DESC) AS _w
  FROM _t1
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  ndistinct_searchid AS n_searches
FROM _t
WHERE
  _w = 1
