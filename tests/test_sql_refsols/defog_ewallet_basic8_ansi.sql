WITH _t1 AS (
  SELECT
    COUNT(txid) AS agg_0,
    SUM(amount) AS agg_1,
    coupon_id
  FROM main.wallet_transactions_daily
  GROUP BY
    coupon_id
), _t0_2 AS (
  SELECT
    coupons.code AS coupon_code,
    COALESCE(_t1.agg_0, 0) AS ordering_2,
    COALESCE(_t1.agg_0, 0) AS redemption_count,
    COALESCE(_t1.agg_1, 0) AS total_discount
  FROM main.coupons AS coupons
  LEFT JOIN _t1 AS _t1
    ON _t1.coupon_id = coupons.cid
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  coupon_code,
  redemption_count,
  total_discount
FROM _t0_2
ORDER BY
  ordering_2 DESC
