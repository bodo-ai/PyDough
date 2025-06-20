WITH _t0 AS (
  SELECT
    ANY_VALUE(_s0.user_name) AS agg_2,
    COUNT(DISTINCT _s10.user_id) AS n_other_users,
    ANY_VALUE(_s0.user_name) AS user_name
  FROM users AS _s0
  JOIN searches AS _s1
    ON _s0.user_id = _s1.search_user_id
  JOIN events AS _s4
    ON LOWER(_s1.search_string) LIKE CONCAT('%', LOWER(_s4.ev_name), '%')
  JOIN searches AS _s7
    ON LOWER(_s7.search_string) LIKE CONCAT('%', LOWER(_s4.ev_name), '%')
  JOIN users AS _s10
    ON _s10.user_id = _s7.search_user_id
  WHERE
    _s0.user_name <> _s10.user_name
  GROUP BY
    _s0.user_id
)
SELECT
  user_name,
  n_other_users
FROM _t0
ORDER BY
  n_other_users DESC,
  agg_2
LIMIT 7
