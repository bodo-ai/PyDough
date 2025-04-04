WITH _t1 AS (
  SELECT
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
), _t0 AS (
  SELECT
    sbtickerid AS _id,
    sbtickersymbol AS symbol
  FROM main.sbticker
)
SELECT
  _id AS _id,
  symbol AS symbol
FROM _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      _id = ticker_id
  )
