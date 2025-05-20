WITH _s4 AS (
  SELECT
    users.user_id AS user_id,
    users.user_name AS user_name
  FROM users AS users
), _s2 AS (
  SELECT
    searches.search_string AS search_string,
    searches.search_user_id AS user_id
  FROM searches AS searches
), _s0 AS (
  SELECT
    events.ev_dt AS date_time,
    events.ev_name AS name
  FROM events AS events
), _t1 AS (
  SELECT
    eras.er_end_year AS end_year,
    eras.er_name AS name,
    eras.er_start_year AS start_year
  FROM eras AS eras
  WHERE
    eras.er_name = 'Cold War'
), _s1 AS (
  SELECT
    _t1.end_year AS end_year,
    _t1.start_year AS start_year
  FROM _t1 AS _t1
), _s3 AS (
  SELECT
    _s0.name AS name
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s1.end_year > EXTRACT(YEAR FROM _s0.date_time)
    AND _s1.start_year <= EXTRACT(YEAR FROM _s0.date_time)
), _t0 AS (
  SELECT
    _s2.user_id AS user_id
  FROM _s2 AS _s2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
      WHERE
        LOWER(_s2.search_string) LIKE CONCAT('%', LOWER(_s3.name), '%')
    )
), _s5 AS (
  SELECT
    COUNT() AS n_cold_war_searches,
    _t0.user_id AS user_id
  FROM _t0 AS _t0
  GROUP BY
    _t0.user_id
)
SELECT
  _s4.user_name AS user_name,
  _s5.n_cold_war_searches AS n_cold_war_searches
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.user_id = _s5.user_id
ORDER BY
  n_cold_war_searches DESC,
  user_name
LIMIT 3
