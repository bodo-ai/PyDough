WITH _t0 AS (
  SELECT
    sbdailyprice.sbdpdate AS date,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
), _s1 AS (
  SELECT
    _t0.ticker_id AS ticker_id
  FROM _t0 AS _t0
  WHERE
    _t0.date >= '2023-04-01'
), _s0 AS (
  SELECT
    sbticker.sbtickerid AS _id
  FROM main.sbticker AS sbticker
)
SELECT
  _s0._id AS _id
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0._id = _s1.ticker_id
  )
