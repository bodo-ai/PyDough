WITH _s0 AS (
  SELECT
    user_id,
    user_name
  FROM users
), _t2 AS (
  SELECT
    ANY_VALUE(_s0.user_id) AS anything_user_id,
    ANY_VALUE(_s0.user_name) AS anything_user_name,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.user_id = searches.search_user_id
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN searches AS searches_2
    ON LOWER(searches_2.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN _s0 AS _s7
    ON _s0.user_name <> _s7.user_name AND _s7.user_id = searches_2.search_user_id
  GROUP BY
    searches.search_id,
    _s0.user_id
), _t0 AS (
  SELECT
    ANY_VALUE(anything_user_name) AS anything_anything_user_name,
    COUNT(*) AS n_searches
  FROM _t2
  WHERE
    n_rows > 0
  GROUP BY
    anything_user_id
)
SELECT
  anything_anything_user_name AS user_name,
  n_searches
FROM _t0
ORDER BY
  n_searches DESC,
  anything_anything_user_name
LIMIT 4
