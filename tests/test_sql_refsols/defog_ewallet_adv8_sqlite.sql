SELECT
  mid AS merchants_id,
  name AS merchants_name,
  category,
  COALESCE(agg_0, 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(agg_0, 0) DESC) AS mrr
FROM (
  SELECT
    agg_0,
    category,
    mid,
    name
  FROM (
    SELECT
      category,
      mid,
      name
    FROM main.merchants
  )
  INNER JOIN (
    SELECT
      SUM(amount) AS agg_0,
      receiver_id
    FROM (
      SELECT
        amount,
        receiver_id
      FROM (
        SELECT
          amount,
          receiver_id,
          receiver_type,
          status
        FROM main.wallet_transactions_daily
      )
      WHERE
        (
          receiver_type = 1
        ) AND (
          status = 'success'
        )
    )
    GROUP BY
      receiver_id
  )
    ON mid = receiver_id
)
