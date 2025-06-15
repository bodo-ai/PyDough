WITH _s1 AS (
  SELECT
    search_string,
    search_user_id AS user_id
  FROM searches
), _t0 AS (
  SELECT
    MAX(users.user_name) AS agg_2,
    COUNT(DISTINCT users_2.user_id) AS n_other_users
  FROM users AS users
  JOIN _s1 AS _s1
    ON _s1.user_id = users.user_id
  JOIN events AS events
    ON LOWER(_s1.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN _s1 AS _s5
    ON LOWER(_s5.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN users AS users_2
    ON _s5.user_id = users_2.user_id
  WHERE
    users.user_name <> users_2.user_name
  GROUP BY
    users.user_id
)
SELECT
  agg_2 AS user_name,
  n_other_users
FROM _t0
ORDER BY
  n_other_users DESC,
  agg_2
LIMIT 7
