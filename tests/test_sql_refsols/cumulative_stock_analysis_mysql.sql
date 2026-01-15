SELECT
  sbTransaction.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY CAST(CAST(sbTransaction.sbtxdatetime AS DATETIME) AS DATE) ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(
    CASE WHEN sbTransaction.sbtxtype = 'buy' THEN sbTransaction.sbtxtype ELSE NULL END
  ) OVER (PARTITION BY CAST(CAST(sbTransaction.sbtxdatetime AS DATETIME) AS DATE) ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    (
      100.0 * SUM(sbTicker.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / COUNT(*) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS pct_apple_txns,
  SUM(
    CASE
      WHEN sbTransaction.sbtxtype = 'buy'
      THEN sbTransaction.sbtxshares
      ELSE 0 - sbTransaction.sbtxshares
    END
  ) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
  ROUND(
    AVG(CAST(sbTransaction.sbtxamount AS DOUBLE)) OVER (ORDER BY sbTransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS rolling_avg_amount
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
WHERE
  EXTRACT(MONTH FROM CAST(sbTransaction.sbtxdatetime AS DATETIME)) = 4
  AND EXTRACT(YEAR FROM CAST(sbTransaction.sbtxdatetime AS DATETIME)) = 2023
  AND sbTransaction.sbtxstatus = 'success'
ORDER BY
  1
