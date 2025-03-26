WITH _table_alias_0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    wallet_transactions_daily.sender_id AS sender_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.sender_type = 0
  GROUP BY
    wallet_transactions_daily.sender_id
)
SELECT
  _table_alias_0.uid AS user_id,
  COALESCE(_table_alias_1.agg_0, 0) AS total_transactions
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.uid = _table_alias_1.sender_id
