WITH _t0 AS (
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
), _table_alias_0 AS (
  SELECT
    merchants.mid AS mid
  FROM main.merchants AS merchants
)
SELECT
  _table_alias_0.mid AS merchant
FROM _table_alias_0 AS _table_alias_0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _table_alias_1 AS _table_alias_1
    WHERE
      _table_alias_0.mid = _table_alias_1.receiver_id
  )
