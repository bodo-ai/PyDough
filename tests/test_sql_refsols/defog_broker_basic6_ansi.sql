WITH _t0 AS (
  SELECT
    sbdailyprice.sbdpdate AS sbdpdate,
    sbdailyprice.sbdptickerid AS sbdptickerid
  FROM main.sbdailyprice AS sbdailyprice
), _s1 AS (
  SELECT
    _t0.sbdptickerid AS sbdptickerid
  FROM _t0 AS _t0
  WHERE
    _t0.sbdpdate >= CAST('2023-04-01' AS DATE)
), _s0 AS (
  SELECT
    sbticker.sbtickerid AS sbtickerid
  FROM main.sbticker AS sbticker
)
SELECT
  _s0.sbtickerid AS _id
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.sbtickerid = _s1.sbdptickerid
  )
