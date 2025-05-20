WITH _s0 AS (
  SELECT
    events.ev_dt AS date_time
  FROM events AS events
), _t1 AS (
  SELECT
    times.t_end_hour AS end_hour,
    times.t_name AS name,
    times.t_start_hour AS start_hour
  FROM times AS times
  WHERE
    times.t_name = 'Pre-Dawn'
), _s1 AS (
  SELECT
    _t1.end_hour AS end_hour,
    _t1.start_hour AS start_hour
  FROM _t1 AS _t1
), _s2 AS (
  SELECT
    _s0.date_time AS date_time
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s1.end_hour > CAST(STRFTIME('%H', _s0.date_time) AS INTEGER)
        AND _s1.start_hour <= CAST(STRFTIME('%H', _s0.date_time) AS INTEGER)
    )
), _t2 AS (
  SELECT
    eras.er_end_year AS end_year,
    eras.er_name AS name,
    eras.er_start_year AS start_year
  FROM eras AS eras
  WHERE
    eras.er_name = 'Cold War'
), _s3 AS (
  SELECT
    _t2.end_year AS end_year,
    _t2.start_year AS start_year
  FROM _t2 AS _t2
), _t0 AS (
  SELECT
    1 AS _
  FROM _s2 AS _s2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
      WHERE
        _s3.end_year > CAST(STRFTIME('%Y', _s2.date_time) AS INTEGER)
        AND _s3.start_year <= CAST(STRFTIME('%Y', _s2.date_time) AS INTEGER)
    )
)
SELECT
  COUNT() AS n_events
FROM _t0 AS _t0
