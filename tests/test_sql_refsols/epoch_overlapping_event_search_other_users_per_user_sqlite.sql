WITH _s0 AS (
  SELECT
    user_id,
    user_name
  FROM users
), _s1 AS (
  SELECT
    search_string,
    search_user_id
  FROM searches
), _t0 AS (
  SELECT
    MAX(_s0.user_name) AS anything_user_name,
    COUNT(DISTINCT _s7.user_id) AS ndistinct_user_id_11
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.user_id = _s1.search_user_id
  JOIN events AS events
    ON LOWER(_s1.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN _s1 AS _s5
    ON LOWER(_s5.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN _s0 AS _s7
    ON _s0.user_name <> _s7.user_name AND _s5.search_user_id = _s7.user_id
  GROUP BY
    _s0.user_id
)
SELECT
  anything_user_name AS user_name,
  ndistinct_user_id_11 AS n_other_users
FROM _t0
ORDER BY
  n_other_users DESC,
  anything_user_name
LIMIT 7
