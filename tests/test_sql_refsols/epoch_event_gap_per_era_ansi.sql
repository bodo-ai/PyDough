WITH _s0 AS (
  SELECT
    eras.er_end_year AS end_year,
    eras.er_name AS name,
    eras.er_start_year AS start_year
  FROM eras AS eras
), _s1 AS (
  SELECT
    events.ev_dt AS date_time
  FROM events AS events
), _s6 AS (
  SELECT
    _s0.name AS name,
    _s0.start_year AS start_year
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.end_year > EXTRACT(YEAR FROM _s1.date_time)
        AND _s0.start_year <= EXTRACT(YEAR FROM _s1.date_time)
    )
), _s4 AS (
  SELECT
    _s2.end_year AS end_year,
    _s2.name AS name,
    _s2.start_year AS start_year
  FROM _s0 AS _s2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s3
      WHERE
        _s2.end_year > EXTRACT(YEAR FROM _s3.date_time)
        AND _s2.start_year <= EXTRACT(YEAR FROM _s3.date_time)
    )
), _t2 AS (
  SELECT
    _s5.date_time AS date_time,
    _s4.name AS name
  FROM _s4 AS _s4
  JOIN _s1 AS _s5
    ON _s4.end_year > EXTRACT(YEAR FROM _s5.date_time)
    AND _s4.start_year <= EXTRACT(YEAR FROM _s5.date_time)
), _t1 AS (
  SELECT
    DATEDIFF(
      _t2.date_time,
      LAG(_t2.date_time, 1) OVER (PARTITION BY _t2.name ORDER BY _t2.date_time NULLS LAST),
      DAY
    ) AS day_gap,
    _t2.name AS name
  FROM _t2 AS _t2
), _s7 AS (
  SELECT
    AVG(_t1.day_gap) AS agg_0,
    _t1.name AS name
  FROM _t1 AS _t1
  GROUP BY
    _t1.name
), _t0 AS (
  SELECT
    _s6.name AS name,
    _s7.agg_0 AS agg_0,
    _s6.start_year AS start_year
  FROM _s6 AS _s6
  LEFT JOIN _s7 AS _s7
    ON _s6.name = _s7.name
)
SELECT
  _t0.name AS era_name,
  _t0.agg_0 AS avg_event_gap
FROM _t0 AS _t0
ORDER BY
  _t0.start_year
