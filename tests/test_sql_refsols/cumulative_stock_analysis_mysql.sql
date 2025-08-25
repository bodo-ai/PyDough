SELECT
  sbtransaction.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY CAST(CAST(sbtransaction.sbtxdatetime AS DATETIME) AS DATE) ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(
    CASE WHEN sbtransaction.sbtxtype = 'buy' THEN sbtransaction.sbtxtype ELSE NULL END
  ) OVER (PARTITION BY CAST(CAST(sbtransaction.sbtxdatetime AS DATETIME) AS DATE) ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    (
      100.0 * SUM(sbticker.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS pct_apple_txns,
  SUM(
    CASE
      WHEN sbtransaction.sbtxtype = 'buy'
      THEN sbtransaction.sbtxshares
      ELSE 0 - sbtransaction.sbtxshares
    END
  ) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
  ROUND(
    AVG(sbtransaction.sbtxamount) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS rolling_avg_amount
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
WHERE
  EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) = 4
  AND EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) = 2023
  AND sbtransaction.sbtxstatus = 'success'
ORDER BY
  1
