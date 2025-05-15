WITH _s4 AS (
  SELECT
    users.user_id AS user_id,
    users.user_name AS user_name
  FROM users AS users
), _s0 AS (
  SELECT
    searches.search_user_id AS user_id
  FROM searches AS searches
), _s1 AS (
  SELECT
    1 AS _
  FROM events AS events
), _t1 AS (
  SELECT
    eras.er_name AS name
  FROM eras AS eras
  WHERE
    eras.er_name = 'Cold War'
), _s2 AS (
  SELECT
    1 AS _
  FROM _t1 AS _t1
), _s3 AS (
  SELECT
    1 AS _
  FROM _s1 AS _s1
  CROSS JOIN _s2 AS _s2
), _t0 AS (
  SELECT
    _s0.user_id AS user_id
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
    )
), _s5 AS (
  SELECT
    COUNT() AS n_cold_war_searches,
    _t0.user_id AS user_id
  FROM _t0 AS _t0
  GROUP BY
    _t0.user_id
)
SELECT
  _s4.user_name AS user_name,
  _s5.n_cold_war_searches AS n_cold_war_searches
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.user_id = _s5.user_id
ORDER BY
  n_cold_war_searches DESC,
  user_name
LIMIT 3
