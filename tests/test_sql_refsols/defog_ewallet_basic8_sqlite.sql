WITH _table_alias_1 AS (
  SELECT
    COUNT(wallet_transactions_daily.txid) AS agg_0,
    SUM(wallet_transactions_daily.amount) AS agg_1,
    wallet_transactions_daily.coupon_id AS coupon_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  GROUP BY
    wallet_transactions_daily.coupon_id
), _t0 AS (
  SELECT
    coupons.code AS coupon_code,
    COALESCE(_table_alias_1.agg_0, 0) AS ordering_2,
    COALESCE(_table_alias_1.agg_0, 0) AS redemption_count,
    COALESCE(_table_alias_1.agg_1, 0) AS total_discount
  FROM main.coupons AS coupons
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.coupon_id = coupons.cid
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  _t0.coupon_code AS coupon_code,
  _t0.redemption_count AS redemption_count,
  _t0.total_discount AS total_discount
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_2 DESC
