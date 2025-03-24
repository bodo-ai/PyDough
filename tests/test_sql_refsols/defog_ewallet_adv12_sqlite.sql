SELECT
  cid AS coupon_id,
  COALESCE(agg_0, 0) AS total_discount
FROM (
  SELECT
    agg_0,
    cid
  FROM (
    SELECT
      cid
    FROM (
      SELECT
        cid,
        merchant_id
      FROM main.coupons
    )
    WHERE
      merchant_id = '1'
  )
  LEFT JOIN (
    SELECT
      SUM(amount) AS agg_0,
      coupon_id
    FROM (
      SELECT
        amount,
        coupon_id
      FROM main.wallet_transactions_daily
    )
    GROUP BY
      coupon_id
  )
    ON cid = coupon_id
)
