WITH _t2 AS (
  SELECT
    COUNT(*) AS agg_0,
    MAX(users.user_id) AS agg_10,
    MAX(users.user_name) AS agg_8
  FROM users AS users
  JOIN searches AS searches
    ON searches.search_user_id = users.user_id
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN searches AS searches_2
    ON LOWER(searches_2.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN users AS users_2
    ON searches_2.search_user_id = users_2.user_id
    AND users.user_name <> users_2.user_name
  GROUP BY
    searches.search_id,
    users.user_id
), _t0 AS (
  SELECT
    MAX(agg_8) AS agg_2,
    COUNT(*) AS n_searches,
    MAX(agg_8) AS user_name
  FROM _t2
  WHERE
    agg_0 > 0
  GROUP BY
    agg_10
)
SELECT
  user_name,
  n_searches
FROM _t0
ORDER BY
  n_searches DESC,
  agg_2
LIMIT 4
