WITH _t1 AS (
  SELECT
    ANY_VALUE(searches.search_user_id) AS anything_search_user_id
  FROM searches AS searches
  JOIN events AS events
    ON CONTAINS(LOWER(searches.search_string), LOWER(events.ev_name))
  JOIN eras AS eras
    ON eras.er_end_year > YEAR(CAST(events.ev_dt AS TIMESTAMP))
    AND eras.er_name = 'Cold War'
    AND eras.er_start_year <= YEAR(CAST(events.ev_dt AS TIMESTAMP))
  GROUP BY
    searches.search_id
), _s5 AS (
  SELECT
    anything_search_user_id,
    COUNT(*) AS n_rows
  FROM _t1
  GROUP BY
    1
)
SELECT
  users.user_name,
  _s5.n_rows AS n_cold_war_searches
FROM users AS users
JOIN _s5 AS _s5
  ON _s5.anything_search_user_id = users.user_id
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 3
