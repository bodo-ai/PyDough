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
  ) AS _table_alias_0
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
      ) AS _t2
      WHERE
        (
          receiver_type = 1
        ) AND (
          status = 'success'
        )
    ) AS _t1
    GROUP BY
      receiver_id
  ) AS _table_alias_1
    ON mid = receiver_id
) AS _t0
