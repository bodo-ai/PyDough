WITH _u_0 AS (
  SELECT
    sbdailyprice.sbdptickerid AS _u_1
  FROM main.sbdailyprice AS sbdailyprice
  GROUP BY
    sbdailyprice.sbdptickerid
)
SELECT
  sbticker.sbtickerid AS _id,
  sbticker.sbtickersymbol AS symbol
FROM main.sbticker AS sbticker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbticker.sbtickerid
WHERE
  _u_0._u_1 IS NULL
