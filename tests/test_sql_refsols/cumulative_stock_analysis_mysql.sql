WITH _t0 AS (
  SELECT
    COUNT(
      CASE WHEN sbTransaction.sbtxtype = 'buy' THEN sbTransaction.sbtxtype ELSE NULL END
    ) OVER (PARTITION BY DATE(CAST(sbTransaction.sbtxdatetime AS DATETIME)) ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
    ROUND(
      (
        100.0 * SUM(sbTicker.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) / COUNT(*) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS pct_apple_txns,
    ROUND(
      AVG(sbTransaction.sbtxamount) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS rolling_avg_amount,
    SUM(
      CASE
        WHEN sbTransaction.sbtxtype = 'buy'
        THEN sbTransaction.sbtxshares
        ELSE 0 - sbTransaction.sbtxshares
      END
    ) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
    COUNT(*) OVER (PARTITION BY DATE(CAST(sbTransaction.sbtxdatetime AS DATETIME)) ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
    sbTransaction.sbtxdatetime AS sbTxDateTime
  FROM main.sbTransaction AS sbTransaction
  JOIN main.sbTicker AS sbTicker
    ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  WHERE
    MONTH(sbTransaction.sbtxdatetime) = 4
    AND YEAR(sbTransaction.sbtxdatetime) = 2023
    AND sbTransaction.sbtxstatus = 'success'
)
SELECT
  sbTxDateTime AS date_time,
  txn_within_day,
  n_buys_within_day,
  pct_apple_txns,
  share_change,
  rolling_avg_amount
FROM _t0
ORDER BY
  sbTxDateTime
