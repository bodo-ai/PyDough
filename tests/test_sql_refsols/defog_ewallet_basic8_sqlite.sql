WITH _s1 AS (
  SELECT
    COUNT(txid) AS count_txid,
    SUM(amount) AS sum_amount,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
), _t0 AS (
  SELECT
    coupons.code AS code_1,
    COALESCE(_s1.count_txid, 0) AS redemption_count_1,
    _s1.sum_amount
  FROM main.coupons AS coupons
  LEFT JOIN _s1 AS _s1
    ON _s1.coupon_id = coupons.cid
  ORDER BY
    COALESCE(_s1.count_txid, 0) DESC
  LIMIT 3
)
SELECT
  code_1 AS coupon_code,
  redemption_count_1 AS redemption_count,
  COALESCE(sum_amount, 0) AS total_discount
FROM _t0
ORDER BY
  redemption_count_1 DESC
