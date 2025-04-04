WITH _t0 AS (
  SELECT
    receiver_id AS receiver_id,
    receiver_type AS receiver_type
  FROM main.wallet_transactions_daily
  WHERE
    receiver_type = 1
), _t1 AS (
  SELECT
    receiver_id AS receiver_id
  FROM _t0
), _t0_2 AS (
  SELECT
    mid AS mid
  FROM main.merchants
)
SELECT
  _t0.mid AS merchant
FROM _t0_2 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      mid = receiver_id
  )
