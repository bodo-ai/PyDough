WITH _u_0 AS (
  SELECT
    sbDpTickerId AS _u_1
  FROM broker.sbDailyPrice
  WHERE
    sbDpDate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    1
)
SELECT
  sbTicker.sbTickerId AS _id
FROM broker.sbTicker AS sbTicker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbTicker.sbTickerId
WHERE
  NOT _u_0._u_1 IS NULL
