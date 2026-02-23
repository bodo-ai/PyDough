WITH _u_0 AS (
  SELECT
    sbdptickerid AS _u_1
  FROM broker.sbDailyPrice
  GROUP BY
    1
)
SELECT
  sbTicker.sbtickerid AS _id,
  sbTicker.sbtickersymbol AS symbol
FROM broker.sbTicker AS sbTicker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbTicker.sbtickerid
WHERE
  _u_0._u_1 IS NULL
