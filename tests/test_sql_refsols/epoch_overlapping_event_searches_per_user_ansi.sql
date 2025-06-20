WITH _t2 AS (
  SELECT
    COUNT(*) AS agg_0,
    ANY_VALUE(_s0.user_id) AS agg_10,
    ANY_VALUE(_s0.user_name) AS agg_8
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
    _s1.search_id,
    _s0.user_id
), _t0 AS (
  SELECT
    ANY_VALUE(agg_8) AS agg_2,
    COUNT(*) AS n_searches,
    ANY_VALUE(agg_8) AS user_name
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
