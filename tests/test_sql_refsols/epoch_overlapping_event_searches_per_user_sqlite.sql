WITH _s0 AS (
  SELECT
    user_id,
    user_name
  FROM users
), _t2 AS (
  SELECT
    MAX(searches.search_user_id) AS anything_search_user_id,
    MAX(_s0.user_name) AS anything_user_name,
    _s0.user_id
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.user_id = searches.search_user_id
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN searches AS searches_2
    ON LOWER(searches_2.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN _s0 AS _s7
    ON _s0.user_name <> _s7.user_name AND _s7.user_id = searches_2.search_user_id
  GROUP BY
    searches.search_id,
    _s0.user_id
)
SELECT
  MAX(anything_user_name) AS user_name,
  COUNT(*) AS n_searches
FROM _t2
WHERE
  anything_search_user_id = user_id
GROUP BY
  user_id
ORDER BY
  n_searches DESC,
  MAX(anything_user_name)
LIMIT 4
