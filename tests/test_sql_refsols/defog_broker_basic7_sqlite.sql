WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    sbtxstatus AS status
  FROM main.sbtransaction
  GROUP BY
    sbtxstatus
)
SELECT
  status,
  COALESCE(agg_0, 0) AS num_transactions
FROM _t1
ORDER BY
  num_transactions DESC
LIMIT 3
