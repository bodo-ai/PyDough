SELECT
  coupon_code,
  redemption_count,
  total_discount
FROM (
  SELECT
    coupon_code,
    ordering_2,
    redemption_count,
    total_discount
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS redemption_count,
      COALESCE(agg_1, 0) AS total_discount,
      code AS coupon_code
    FROM (
      SELECT
        agg_0,
        agg_1,
        code
      FROM (
        SELECT
          cid,
          code
        FROM main.coupons
      )
      LEFT JOIN (
        SELECT
          COUNT(txid) AS agg_0,
          SUM(amount) AS agg_1,
          coupon_id
        FROM (
          SELECT
            amount,
            coupon_id,
            txid
          FROM main.wallet_transactions_daily
        )
        GROUP BY
          coupon_id
      )
        ON cid = coupon_id
    )
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
ORDER BY
  ordering_2 DESC
