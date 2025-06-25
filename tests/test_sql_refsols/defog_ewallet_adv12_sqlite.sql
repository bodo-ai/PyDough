WITH _s1 AS (
  SELECT
    SUM(amount) AS sum_amount,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
)
SELECT
  coupons.cid AS coupon_id,
  COALESCE(_s1.sum_amount, 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _s1 AS _s1
  ON _s1.coupon_id = coupons.cid
WHERE
  coupons.merchant_id = '1'
