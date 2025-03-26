WITH _table_alias_0 AS (
  SELECT
    merchants.mid AS mid
  FROM main.merchants AS merchants
), _t0 AS (
  SELECT
    wallet_transactions_daily.receiver_id AS receiver_id,
    wallet_transactions_daily.receiver_type AS receiver_type
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.receiver_type = 1
), _table_alias_1 AS (
  SELECT
    _t0.receiver_id AS receiver_id
  FROM _t0 AS _t0
), _u_0 AS (
  SELECT
    _table_alias_1.receiver_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.receiver_id
)
SELECT
  _table_alias_0.mid AS merchant
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0.mid = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
