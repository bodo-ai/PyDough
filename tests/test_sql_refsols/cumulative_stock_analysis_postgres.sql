SELECT
  sbtransaction.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(
    CASE WHEN sbtransaction.sbtxtype = 'buy' THEN sbtransaction.sbtxtype ELSE NULL END
  ) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    CAST((
      100.0 * SUM(CASE WHEN sbticker.sbtickersymbol IN ('AAPL', 'AMZN') THEN 1 ELSE 0 END) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS DOUBLE PRECISION) / COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
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
  EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) = 4
  AND EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) = 2023
  AND sbtransaction.sbtxstatus = 'success'
ORDER BY
  1 NULLS FIRST
