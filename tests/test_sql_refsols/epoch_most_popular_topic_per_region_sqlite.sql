WITH _t1 AS (
  SELECT
    COUNT(DISTINCT _s1.search_id) AS n_searches,
    _s0.ev_typ AS event_type,
    _s4.user_region AS region
  FROM events AS _s0
  JOIN searches AS _s1
    ON LOWER(_s1.search_string) LIKE (
      '%' || LOWER(_s0.ev_name) || '%'
    )
  JOIN users AS _s4
    ON _s1.search_user_id = _s4.user_id
  GROUP BY
    _s0.ev_typ,
    _s4.user_region
), _t AS (
  SELECT
    n_searches,
    event_type,
    region,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY n_searches DESC) AS _w
  FROM _t1
)
SELECT
  region,
  event_type,
  n_searches
FROM _t
WHERE
  _w = 1
