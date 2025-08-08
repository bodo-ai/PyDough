WITH _s1 AS (
  SELECT
    COUNT(txid) AS count_txid,
    SUM(amount) AS sum_amount,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
)
SELECT
  coupons.code AS coupon_code,
  COALESCE(_s1.count_txid, 0) AS redemption_count,
  COALESCE(_s1.sum_amount, 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _s1 AS _s1
  ON _s1.coupon_id = coupons.cid
ORDER BY
  COALESCE(_s1.count_txid, 0) DESC NULLS LAST
LIMIT 3
