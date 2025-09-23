WITH _s1 AS (
  SELECT
    coupon_id,
    COUNT(txid) AS count_txid,
    SUM(amount) AS sum_amount
  FROM main.wallet_transactions_daily
  GROUP BY
    1
)
SELECT
  coupons.code AS coupon_code,
  _s1.count_txid AS redemption_count,
  COALESCE(_s1.sum_amount, 0) AS total_discount
FROM main.coupons AS coupons
JOIN _s1 AS _s1
  ON _s1.coupon_id = coupons.cid
ORDER BY
  2 DESC
LIMIT 3
