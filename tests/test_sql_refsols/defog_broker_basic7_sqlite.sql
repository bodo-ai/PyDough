SELECT
  status,
  num_transactions
FROM (
  SELECT
    num_transactions,
    ordering_1,
    status
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_transactions,
      COALESCE(agg_0, 0) AS ordering_1,
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
    ordering_1 DESC
  LIMIT 3
)
ORDER BY
  ordering_1 DESC
