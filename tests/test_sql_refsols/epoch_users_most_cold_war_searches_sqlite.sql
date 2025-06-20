WITH _t0 AS (
  SELECT
    MAX(searches.search_user_id) AS agg_5
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
    COUNT(*) AS n_cold_war_searches,
    agg_5
  FROM _t0
  GROUP BY
    agg_5
)
SELECT
  users.user_name,
  _s5.n_cold_war_searches
FROM users AS users
JOIN _s5 AS _s5
  ON _s5.agg_5 = users.user_id
ORDER BY
  n_cold_war_searches DESC,
  user_name
LIMIT 3
