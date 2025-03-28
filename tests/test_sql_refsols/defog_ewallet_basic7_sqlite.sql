SELECT
  status,
  count
FROM (
  SELECT
    COALESCE(agg_0, 0) AS count,
    status
  FROM (
    SELECT
      COUNT() AS agg_0,
      status
    FROM (
      SELECT
        status
      FROM main.wallet_transactions_daily
    )
    GROUP BY
      status
  )
)
ORDER BY
  count DESC
LIMIT 3
