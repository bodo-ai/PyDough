WITH _table_alias_0 AS (
  SELECT
    coupons.cid AS cid
  FROM main.coupons AS coupons
  WHERE
    coupons.merchant_id = '1'
), _table_alias_1 AS (
  SELECT
    SUM(wallet_transactions_daily.amount) AS agg_0,
    wallet_transactions_daily.coupon_id AS coupon_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  GROUP BY
    wallet_transactions_daily.coupon_id
)
SELECT
  _table_alias_0.cid AS coupon_id,
  COALESCE(_table_alias_1.agg_0, 0) AS total_discount
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.cid = _table_alias_1.coupon_id
