WITH _s1 AS (
  SELECT
    sbdptickerid
  FROM main.sbdailyprice
)
SELECT
  sbticker.sbtickerid AS _id,
  sbticker.sbtickersymbol AS symbol
FROM main.sbticker AS sbticker
ANTI JOIN _s1 AS _s1
  ON _s1.sbdptickerid = sbticker.sbtickerid
