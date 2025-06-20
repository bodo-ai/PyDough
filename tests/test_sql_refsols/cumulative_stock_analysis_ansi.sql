SELECT
  _s0.sbtxdatetime AS date_time,
  COUNT(*) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(_s0.sbtxdatetime AS TIMESTAMP)) ORDER BY _s0.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
  COUNT(CASE WHEN _s0.sbtxtype = 'buy' THEN _s0.sbtxtype ELSE NULL END) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(_s0.sbtxdatetime AS TIMESTAMP)) ORDER BY _s0.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
  ROUND(
    (
      100.0 * SUM(_s1.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY _s0.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / COUNT(*) OVER (ORDER BY _s0.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS pct_apple_txns,
  SUM(CASE WHEN _s0.sbtxtype = 'buy' THEN _s0.sbtxshares ELSE 0 - _s0.sbtxshares END) OVER (ORDER BY _s0.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
  ROUND(
    AVG(_s0.sbtxamount) OVER (ORDER BY _s0.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2
  ) AS rolling_avg_amount
FROM main.sbtransaction AS _s0
JOIN main.sbticker AS _s1
  ON _s0.sbtxtickerid = _s1.sbtickerid
WHERE
  EXTRACT(MONTH FROM _s0.sbtxdatetime) = 4
  AND EXTRACT(YEAR FROM _s0.sbtxdatetime) = 2023
  AND _s0.sbtxstatus = 'success'
ORDER BY
  date_time
