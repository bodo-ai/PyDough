WITH _t1 AS (
  SELECT
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
), _t0 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
)
SELECT
  _t0._id AS _id,
  _t0.symbol AS symbol
FROM _t0 AS _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0._id = _t1.ticker_id
  )
