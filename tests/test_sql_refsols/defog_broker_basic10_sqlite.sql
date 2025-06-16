WITH _s1 AS (
  SELECT
    sbdailyprice.sbdptickerid AS sbdptickerid
  FROM main.sbdailyprice AS sbdailyprice
), _s0 AS (
  SELECT
    sbticker.sbtickerid AS sbtickerid,
    sbticker.sbtickersymbol AS sbtickersymbol
  FROM main.sbticker AS sbticker
)
SELECT
  _s0.sbtickerid AS _id,
  _s0.sbtickersymbol AS symbol
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.sbtickerid = _s1.sbdptickerid
  )
