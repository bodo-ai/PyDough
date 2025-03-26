WITH _table_alias_0 AS (
  SELECT
    wallet_transactions_daily.created_at AS created_at,
    wallet_transactions_daily.sender_id AS sender_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    CAST(STRFTIME('%Y', wallet_transactions_daily.created_at) AS INTEGER) = 2023
    AND wallet_transactions_daily.sender_type = 0
    AND wallet_transactions_daily.status = 'success'
), _table_alias_1 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
  WHERE
    users.status = 'active'
)
SELECT
  DATE(_table_alias_0.created_at, 'start of month') AS month,
  COUNT(DISTINCT _table_alias_0.sender_id) AS active_users
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.sender_id = _table_alias_1.uid
GROUP BY
  DATE(_table_alias_0.created_at, 'start of month')
