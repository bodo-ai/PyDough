WITH _s1 AS (
  SELECT
    events.ev_dt AS ev_dt
  FROM events AS events
), _t1 AS (
  SELECT
    times.t_end_hour AS t_end_hour,
    times.t_name AS t_name,
    times.t_start_hour AS t_start_hour
  FROM times AS times
), _s2 AS (
  SELECT
    _t1.t_end_hour AS end_hour,
    _t1.t_start_hour AS start_hour
  FROM _t1 AS _t1
  WHERE
    _t1.t_name = 'Pre-Dawn'
), _s0 AS (
  SELECT
    _s1.ev_dt AS date_time
  FROM _s1 AS _s1
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s2 AS _s2
      WHERE
        _s2.end_hour > CAST(STRFTIME('%H', _s1.ev_dt) AS INTEGER)
        AND _s2.start_hour <= CAST(STRFTIME('%H', _s1.ev_dt) AS INTEGER)
    )
), _t2 AS (
  SELECT
    eras.er_end_year AS er_end_year,
    eras.er_name AS er_name,
    eras.er_start_year AS er_start_year
  FROM eras AS eras
), _s3 AS (
  SELECT
    _t2.er_end_year AS end_year,
    _t2.er_start_year AS er_start_year
  FROM _t2 AS _t2
  WHERE
    _t2.er_name = 'Cold War'
), _t0 AS (
  SELECT
    1 AS _
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
      WHERE
        _s3.end_year > CAST(STRFTIME('%Y', _s0.date_time) AS INTEGER)
        AND _s3.er_start_year <= CAST(STRFTIME('%Y', _s0.date_time) AS INTEGER)
    )
)
SELECT
  COUNT() AS n_events
FROM _t0 AS _t0
