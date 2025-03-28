WITH _table_alias_1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(wallet_transactions_daily.amount) AS agg_0,
    wallet_transactions_daily.receiver_id AS receiver_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.created_at >= DATE(DATETIME(DATETIME('now'), '-150 day'), 'start of day')
    AND wallet_transactions_daily.receiver_type = 1
  GROUP BY
    wallet_transactions_daily.receiver_id
), _t0 AS (
  SELECT
    merchants.name AS merchant_name,
    COALESCE(_table_alias_1.agg_0, 0) AS ordering_2,
    COALESCE(_table_alias_1.agg_0, 0) AS total_amount,
    COALESCE(_table_alias_1.agg_1, 0) AS total_transactions
  FROM main.merchants AS merchants
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.receiver_id = merchants.mid
  ORDER BY
    ordering_2 DESC
  LIMIT 2
)
SELECT
  _t0.merchant_name AS merchant_name,
  _t0.total_transactions AS total_transactions,
  _t0.total_amount AS total_amount
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_2 DESC
