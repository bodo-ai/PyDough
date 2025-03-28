WITH _table_alias_1 AS (
  SELECT
    COUNT(DISTINCT wallet_transactions_daily.coupon_id) AS agg_0,
    COUNT(DISTINCT wallet_transactions_daily.txid) AS agg_1,
    wallet_transactions_daily.receiver_id AS receiver_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.status = 'success'
  GROUP BY
    wallet_transactions_daily.receiver_id
)
SELECT
  merchants.name AS name,
  (
    _table_alias_1.agg_0 * 1.0
  ) / _table_alias_1.agg_1 AS CPUR
FROM main.merchants AS merchants
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.receiver_id = merchants.mid
