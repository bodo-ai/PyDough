WITH _s1 AS (
  SELECT
    amount,
    coupon_id
  FROM main.wallet_transactions_daily
)
SELECT
  _s1.coupon_id,
  COALESCE(SUM(_s1.amount), 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _s1 AS _s1
  ON _s1.coupon_id = coupons.cid
WHERE
  coupons.merchant_id = '1'
GROUP BY
  1
