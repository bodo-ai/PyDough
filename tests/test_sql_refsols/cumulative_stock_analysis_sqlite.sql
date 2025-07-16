SELECT
  sbtransaction.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY DATE(sbtransaction.sbtxdatetime, 'start of day') ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(
    CASE WHEN sbtransaction.sbtxtype = 'buy' THEN sbtransaction.sbtxtype ELSE NULL END
  ) OVER (PARTITION BY DATE(sbtransaction.sbtxdatetime, 'start of day') ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    CAST((
      100.0 * SUM(sbticker.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS REAL) / COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS pct_apple_txns,
  SUM(
    IIF(
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
  CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) = 2023
  AND CAST(STRFTIME('%m', sbtransaction.sbtxdatetime) AS INTEGER) = 4
  AND sbtransaction.sbtxstatus = 'success'
ORDER BY
  sbtransaction.sbtxdatetime
