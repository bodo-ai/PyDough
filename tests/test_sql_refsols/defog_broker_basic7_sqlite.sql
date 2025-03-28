SELECT
  status,
  num_transactions
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_transactions,
    status
  FROM (
    SELECT
      COUNT() AS agg_0,
      status
    FROM (
      SELECT
        sbTxStatus AS status
      FROM main.sbTransaction
    )
    GROUP BY
      status
  )
)
ORDER BY
  num_transactions DESC
LIMIT 3
