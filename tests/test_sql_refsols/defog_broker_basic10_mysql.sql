WITH _u_0 AS (
  SELECT
    sbDpTickerId AS _u_1
  FROM broker.sbDailyPrice
  GROUP BY
    1
)
SELECT
  sbTicker.sbTickerId AS _id,
  sbTicker.sbTickerSymbol AS symbol
FROM broker.sbTicker AS sbTicker
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbTicker.sbTickerId
WHERE
  NOT NOT _u_0._u_1 IS NULL
