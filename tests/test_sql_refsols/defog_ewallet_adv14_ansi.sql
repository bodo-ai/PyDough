SELECT
  COALESCE(agg_0, 0) / agg_1 AS _expr0
FROM (
  SELECT
    COUNT() AS agg_1,
    SUM(expr_2) AS agg_0
  FROM (
    SELECT
      status = 'success' AS expr_2
    FROM (
      SELECT
        status
      FROM (
        SELECT
          created_at,
          status
        FROM main.wallet_transactions_daily
      )
      WHERE
        DATEDIFF(CURRENT_TIMESTAMP(), created_at, MONTH) = 1
    )
  )
)
