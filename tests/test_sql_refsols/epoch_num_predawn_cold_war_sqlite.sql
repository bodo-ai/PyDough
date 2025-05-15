WITH _s1 AS (
  SELECT
    1 AS _
  FROM events AS events
), _t1 AS (
  SELECT
    times.t_name AS name
  FROM times AS times
  WHERE
    times.t_name = 'Pre-Dawn'
), _s2 AS (
  SELECT
    1 AS _
  FROM _t1 AS _t1
), _s0 AS (
  SELECT
    1 AS _
  FROM _s1 AS _s1
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s2 AS _s2
    )
), _t2 AS (
  SELECT
    eras.er_name AS name
  FROM eras AS eras
  WHERE
    eras.er_name = 'Cold War'
), _s3 AS (
  SELECT
    1 AS _
  FROM _t2 AS _t2
), _t0 AS (
  SELECT
    1 AS _
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
    )
)
SELECT
  COUNT() AS n_events
FROM _t0 AS _t0
