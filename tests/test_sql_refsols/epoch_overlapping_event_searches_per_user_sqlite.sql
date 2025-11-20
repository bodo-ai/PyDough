WITH _s0 AS (
  SELECT
    user_id,
    user_name
  FROM users
), _t2 AS (
  SELECT
    _s0.user_id,
    MAX(searches.search_user_id) AS anything_searchuserid,
    MAX(_s0.user_name) AS anything_username
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
    1
)
SELECT
  MAX(anything_username) AS user_name,
  COUNT(*) AS n_searches
FROM _t2
WHERE
  anything_searchuserid = user_id
GROUP BY
  user_id
ORDER BY
  2 DESC,
  1
LIMIT 4
