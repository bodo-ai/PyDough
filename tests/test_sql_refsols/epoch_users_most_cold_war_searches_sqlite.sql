WITH _t1 AS (
  SELECT
    MAX(searches.search_user_id) AS anything_searchuserid
  FROM searches AS searches
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  JOIN eras AS eras
    ON eras.er_end_year > CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
    AND eras.er_name = 'Cold War'
    AND eras.er_start_year <= CAST(STRFTIME('%Y', events.ev_dt) AS INTEGER)
  GROUP BY
    searches.search_id
), _s5 AS (
  SELECT
    anything_searchuserid,
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
  ON _s5.anything_searchuserid = users.user_id
ORDER BY
  2 DESC,
  1
LIMIT 3
