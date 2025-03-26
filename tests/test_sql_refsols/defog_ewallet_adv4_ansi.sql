WITH _table_alias_0 AS (
  SELECT
    wallet_transactions_daily.amount AS amount,
    wallet_transactions_daily.sender_id AS sender_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), CAST(wallet_transactions_daily.created_at AS DATETIME), DAY) <= 7
), _table_alias_1 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
  WHERE
    users.country = 'US'
), _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(_table_alias_0.amount) AS agg_1
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.sender_id = _table_alias_1.uid
)
SELECT
  _t0.agg_0 AS num_transactions,
  CASE WHEN _t0.agg_0 > 0 THEN COALESCE(_t0.agg_1, 0) ELSE NULL END AS total_amount
FROM _t0 AS _t0
