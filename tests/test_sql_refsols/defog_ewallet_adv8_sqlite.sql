WITH _table_alias_0 AS (
  SELECT
    merchants.category AS category,
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
), _table_alias_1 AS (
  SELECT
    SUM(wallet_transactions_daily.amount) AS agg_0,
    wallet_transactions_daily.receiver_id AS receiver_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.receiver_type = 1
    AND wallet_transactions_daily.status = 'success'
  GROUP BY
    wallet_transactions_daily.receiver_id
)
SELECT
  _table_alias_0.mid AS merchants_id,
  _table_alias_0.name AS merchants_name,
  _table_alias_0.category AS category,
  COALESCE(_table_alias_1.agg_0, 0) AS total_revenue,
  ROW_NUMBER() OVER (ORDER BY COALESCE(_table_alias_1.agg_0, 0) DESC) AS mrr
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.mid = _table_alias_1.receiver_id
