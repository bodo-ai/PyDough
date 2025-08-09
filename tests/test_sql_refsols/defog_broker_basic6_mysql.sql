WITH _u_0 AS (
  SELECT
    sbdptickerid AS _u_1
  FROM main.sbDailyPrice
  WHERE
    sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    sbdptickerid
)
SELECT
  sbTicker.sbtickerid AS _id
FROM main.sbTicker AS sbTicker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbTicker.sbtickerid
WHERE
  NOT _u_0._u_1 IS NULL
