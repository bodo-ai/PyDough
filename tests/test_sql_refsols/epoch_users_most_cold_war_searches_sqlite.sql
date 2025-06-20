WITH _t0 AS (
  SELECT
    MAX(_s1.search_user_id) AS agg_5
  FROM searches AS _s1
  JOIN events AS _s2
    ON LOWER(_s1.search_string) LIKE (
      '%' || LOWER(_s2.ev_name) || '%'
    )
  JOIN eras AS _s5
    ON _s5.er_end_year > CAST(STRFTIME('%Y', _s2.ev_dt) AS INTEGER)
    AND _s5.er_name = 'Cold War'
    AND _s5.er_start_year <= CAST(STRFTIME('%Y', _s2.ev_dt) AS INTEGER)
  GROUP BY
    _s1.search_id
), _s9 AS (
  SELECT
    COUNT(*) AS n_cold_war_searches,
    agg_5
  FROM _t0
  GROUP BY
    agg_5
)
SELECT
  _s0.user_name,
  _s9.n_cold_war_searches
FROM users AS _s0
JOIN _s9 AS _s9
  ON _s0.user_id = _s9.agg_5
ORDER BY
  n_cold_war_searches DESC,
  user_name
LIMIT 3
