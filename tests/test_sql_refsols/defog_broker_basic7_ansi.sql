WITH _t0 AS (
  SELECT
    COUNT(*) AS num_transactions,
    sbtxstatus AS status
  FROM main.sbtransaction
  GROUP BY
    sbtxstatus
)
SELECT
  status,
  num_transactions
FROM _t0
ORDER BY
  num_transactions DESC
LIMIT 3
