WITH _s0 AS (
  SELECT
    user_id,
    user_name
  FROM users
), _t2 AS (
  SELECT
    ANY_VALUE(searches.search_user_id) AS anything_search_user_id,
    ANY_VALUE(_s0.user_name) AS anything_user_name,
    _s0.user_id
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
    3
)
SELECT
  ANY_VALUE(anything_user_name) COLLATE utf8mb4_bin AS user_name,
  COUNT(*) AS n_searches
FROM _t2
WHERE
  anything_search_user_id = user_id
GROUP BY
  user_id
ORDER BY
  2 DESC,
  1
LIMIT 4
