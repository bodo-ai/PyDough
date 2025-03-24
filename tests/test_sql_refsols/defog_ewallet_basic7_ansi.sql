SELECT
  status,
  count
FROM (
  SELECT
    count,
    ordering_1,
    status
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS count,
      COALESCE(agg_0, 0) AS ordering_1,
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
    ordering_1 DESC
  LIMIT 3
)
ORDER BY
  ordering_1 DESC
