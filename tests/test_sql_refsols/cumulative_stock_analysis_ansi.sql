WITH _t0 AS (
  SELECT
    COUNT(
      CASE WHEN sbtransaction.sbtxtype = 'buy' THEN sbtransaction.sbtxtype ELSE NULL END
    ) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) ORDER BY sbtransaction.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_buys_within_day,
    ROUND(
      (
        100.0 * SUM(sbticker.sbtickersymbol IN ('AAPL', 'AMZN')) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) / COUNT(*) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS pct_apple_txns,
    ROUND(
      AVG(sbtransaction.sbtxamount) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS rolling_avg_amount,
    SUM(
      CASE
        WHEN sbtransaction.sbtxtype = 'buy'
        THEN sbtransaction.sbtxshares
        ELSE 0 - sbtransaction.sbtxshares
      END
    ) OVER (ORDER BY sbtransaction.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS share_change,
    COUNT(*) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) ORDER BY sbtransaction.sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS txn_within_day,
    sbtransaction.sbtxdatetime
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  WHERE
    EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) = 4
    AND EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) = 2023
    AND sbtransaction.sbtxstatus = 'success'
)
SELECT
  sbtxdatetime AS date_time,
  txn_within_day,
  n_buys_within_day,
  pct_apple_txns,
  share_change,
  rolling_avg_amount
FROM _t0
ORDER BY
  sbtxdatetime
