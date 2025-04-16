WITH _t3 AS (
  SELECT
    MAX(users.user_name) AS agg_8,
    MAX(users.user_id) AS agg_10,
    COUNT() AS agg_0
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
), _t1 AS (
  SELECT
    MAX(agg_8) AS agg_2,
    MAX(agg_8) AS user_name,
    COUNT() AS n_searches
  FROM _t3
  WHERE
    agg_0 > 0
  GROUP BY
    agg_10
), _t0 AS (
  SELECT
    agg_2,
    n_searches,
    user_name
  FROM _t1
  ORDER BY
    n_searches DESC,
    agg_2
  LIMIT 4
)
SELECT
  user_name,
  n_searches
FROM _t0
ORDER BY
  n_searches DESC,
  agg_2
