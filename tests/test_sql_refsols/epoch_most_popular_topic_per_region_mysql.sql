WITH _t1 AS (
  SELECT
    EVENTS.ev_typ,
    USERS.user_region,
    COUNT(DISTINCT SEARCHES.search_id) AS ndistinct_searchid
  FROM EVENTS AS EVENTS
  JOIN SEARCHES AS SEARCHES
    ON LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN USERS AS USERS
    ON SEARCHES.search_user_id = USERS.user_id
  GROUP BY
    1,
    2
), _t AS (
  SELECT
    ev_typ,
    user_region,
    ndistinct_searchid,
    ROW_NUMBER() OVER (PARTITION BY user_region ORDER BY CASE WHEN ndistinct_searchid IS NULL THEN 1 ELSE 0 END DESC, ndistinct_searchid DESC) AS _w
  FROM _t1
)
SELECT
  user_region AS region,
  ev_typ AS event_type,
  ndistinct_searchid AS n_searches
FROM _t
WHERE
  _w = 1
