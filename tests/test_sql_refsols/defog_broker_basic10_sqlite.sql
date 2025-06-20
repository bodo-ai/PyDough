SELECT
  _s0.sbtickerid AS _id,
  _s0.sbtickersymbol AS symbol
FROM main.sbticker AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbdailyprice AS _s1
    WHERE
      _s0.sbtickerid = _s1.sbdptickerid
  )
