WITH _table_alias_1 AS (
  SELECT
    SUM(wallet_transactions_daily.amount) AS agg_0,
    wallet_transactions_daily.coupon_id AS coupon_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  GROUP BY
    wallet_transactions_daily.coupon_id
)
SELECT
  coupons.cid AS coupon_id,
  COALESCE(_table_alias_1.agg_0, 0) AS total_discount
FROM main.coupons AS coupons
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.coupon_id = coupons.cid
WHERE
  coupons.merchant_id = '1'
