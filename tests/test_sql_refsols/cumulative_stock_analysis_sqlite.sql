SELECT
  _s0.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY DATE(_s0.sbtxdatetime, 'start of day') ORDER BY _s0.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(CASE WHEN _s0.sbtxtype = 'buy' THEN _s0.sbtxtype ELSE NULL END) OVER (PARTITION BY DATE(_s0.sbtxdatetime, 'start of day') ORDER BY _s0.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    CAST((
      100.0 * SUM(_s1.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY _s0.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS REAL) / COUNT(*) OVER (ORDER BY _s0.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS pct_apple_txns,
  SUM(IIF(_s0.sbtxtype = 'buy', _s0.sbtxshares, 0 - _s0.sbtxshares)) OVER (ORDER BY _s0.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
  ROUND(
    AVG(_s0.sbtxamount) OVER (ORDER BY _s0.sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS rolling_avg_amount
FROM main.sbtransaction AS _s0
JOIN main.sbticker AS _s1
  ON _s0.sbtxtickerid = _s1.sbtickerid
WHERE
  CAST(STRFTIME('%Y', _s0.sbtxdatetime) AS INTEGER) = 2023
  AND CAST(STRFTIME('%m', _s0.sbtxdatetime) AS INTEGER) = 4
  AND _s0.sbtxstatus = 'success'
ORDER BY
  date_time
