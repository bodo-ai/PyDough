WITH _t0 AS (
  SELECT
    wallet_transactions_daily.receiver_id AS receiver_id,
    wallet_transactions_daily.receiver_type AS receiver_type
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.receiver_type = 1
), _t1 AS (
  SELECT
    _t0.receiver_id AS receiver_id
  FROM _t0 AS _t0
), _t0_2 AS (
  SELECT
    merchants.mid AS merchant,
    merchants.mid AS mid
  FROM main.merchants AS merchants
)
SELECT
  _t0.merchant AS merchant
FROM _t0_2 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0.mid = _t1.receiver_id
  )
