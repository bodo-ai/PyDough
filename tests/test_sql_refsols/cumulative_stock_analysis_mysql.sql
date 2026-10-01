SELECT
  sbTransaction.sbTxDateTime AS date_time,
  COUNT(*) OVER (
    PARTITION BY CAST(CAST(sbTransaction.sbTxDateTime AS DATETIME) AS DATE)
    ORDER BY sbTransaction.sbTxDateTime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS txn_within_day,
  COUNT(
    CASE WHEN sbTransaction.sbTxType = 'buy' THEN sbTransaction.sbTxType ELSE NULL END
  ) OVER (
    PARTITION BY CAST(CAST(sbTransaction.sbTxDateTime AS DATETIME) AS DATE)
    ORDER BY sbTransaction.sbTxDateTime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS n_buys_within_day,
  ROUND(
    (
      100.0 * SUM(sbTicker.sbTickerSymbol IN ('AAPL', 'AMZN')) OVER (
        ORDER BY sbTransaction.sbTxDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ) / COUNT(*) OVER (
      ORDER BY sbTransaction.sbTxDateTime
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    2
  ) AS pct_apple_txns,
  SUM(
    CASE
      WHEN sbTransaction.sbTxType = 'buy'
      THEN sbTransaction.sbTxShares
      ELSE (
        -1
      ) * sbTransaction.sbTxShares
    END
  ) OVER (
    ORDER BY sbTransaction.sbTxDateTime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS share_change,
  ROUND(
    AVG(CAST(sbTransaction.sbTxAmount AS DOUBLE)) OVER (
      ORDER BY sbTransaction.sbTxDateTime
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    2
  ) AS rolling_avg_amount
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbTickerId = sbTransaction.sbTxTickerId
WHERE
  EXTRACT(MONTH FROM CAST(sbTransaction.sbTxDateTime AS DATETIME)) = 4
  AND EXTRACT(YEAR FROM CAST(sbTransaction.sbTxDateTime AS DATETIME)) = 2023
  AND sbTransaction.sbTxStatus = 'success'
ORDER BY
  1
