WITH _u_0 AS (
  SELECT
    sbdptickerid AS _u_1
  FROM main.sbdailyprice
  GROUP BY
    1
)
SELECT
  sbticker.sbtickerid AS _id,
  sbticker.sbtickersymbol AS symbol
FROM main.sbticker AS sbticker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbticker.sbtickerid
WHERE
  _u_0._u_1 IS NULL
