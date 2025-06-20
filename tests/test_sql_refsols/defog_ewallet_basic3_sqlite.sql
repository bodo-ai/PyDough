WITH _t0 AS (
  SELECT
    wallet_transactions_daily.receiver_id AS receiver_id,
    wallet_transactions_daily.receiver_type AS receiver_type
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
), _s1 AS (
  SELECT
    _t0.receiver_id AS receiver_id
  FROM _t0 AS _t0
  WHERE
    _t0.receiver_type = 1
), _s0 AS (
  SELECT
    merchants.mid AS merchant,
    merchants.mid AS mid
  FROM main.merchants AS merchants
)
SELECT
  _s0.merchant AS merchant
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.mid = _s1.receiver_id
  )
