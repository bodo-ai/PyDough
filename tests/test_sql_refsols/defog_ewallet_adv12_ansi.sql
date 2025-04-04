WITH _t1_2 AS (
  SELECT
    SUM(amount) AS agg_0,
    coupon_id AS coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
)
SELECT
  coupons.cid AS coupon_id,
  COALESCE(_t1.agg_0, 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _t1_2 AS _t1
  ON _t1.coupon_id = coupons.cid
WHERE
  coupons.merchant_id = '1'
