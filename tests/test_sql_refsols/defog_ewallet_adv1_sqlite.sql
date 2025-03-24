SELECT
  name,
  CAST((
    agg_0 * 1.0
  ) AS REAL) / agg_1 AS CPUR
FROM (
  SELECT
    agg_0,
    agg_1,
    name
  FROM (
    SELECT
      mid,
      name
    FROM main.merchants
  )
  INNER JOIN (
    SELECT
      COUNT(DISTINCT coupon_id) AS agg_0,
      COUNT(DISTINCT txid) AS agg_1,
      receiver_id
    FROM (
      SELECT
        coupon_id,
        receiver_id,
        txid
      FROM (
        SELECT
          coupon_id,
          receiver_id,
          status,
          txid
        FROM main.wallet_transactions_daily
      )
      WHERE
        status = 'success'
    )
    GROUP BY
      receiver_id
  )
    ON mid = receiver_id
)
