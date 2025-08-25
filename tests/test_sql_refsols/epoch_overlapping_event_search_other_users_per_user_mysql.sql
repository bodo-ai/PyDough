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
)
SELECT
  ANY_VALUE(_s0.user_name) COLLATE utf8mb4_bin AS user_name,
  COUNT(DISTINCT _s7.user_id) AS n_other_users
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.user_id = _s1.search_user_id
JOIN events AS events
  ON LOWER(_s1.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
JOIN _s1 AS _s5
  ON LOWER(_s5.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
JOIN _s0 AS _s7
  ON _s0.user_name <> _s7.user_name AND _s5.search_user_id = _s7.user_id
GROUP BY
  _s0.user_id
ORDER BY
  2 DESC,
  1
LIMIT 7
