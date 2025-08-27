SELECT
  sbtransaction.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(
    CASE WHEN sbtransaction.sbtxtype = 'buy' THEN sbtransaction.sbtxtype ELSE NULL END
  ) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    (
      100.0 * COUNT_IF(sbticker.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS pct_apple_txns,
  SUM(
    IFF(
      sbtransaction.sbtxtype = 'buy',
      sbtransaction.sbtxshares,
      0 - sbtransaction.sbtxshares
    )
  ) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
  ROUND(
    AVG(sbtransaction.sbtxamount) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS rolling_avg_amount
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
WHERE
  MONTH(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) = 4
  AND YEAR(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) = 2023
  AND sbtransaction.sbtxstatus = 'success'
ORDER BY
  1 NULLS FIRST
