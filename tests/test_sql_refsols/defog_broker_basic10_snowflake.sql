WITH _u_0 AS (
  SELECT
    sbdptickerid AS _u_1
  FROM MAIN.SBDAILYPRICE
  GROUP BY
    1
)
SELECT
  SBTICKER.sbtickerid AS _id,
  SBTICKER.sbtickersymbol AS symbol
FROM MAIN.SBTICKER AS SBTICKER
LEFT JOIN _u_0 AS _u_0
  ON SBTICKER.sbtickerid = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
