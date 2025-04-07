WITH _t1 AS (
  SELECT
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
), _t0 AS (
  SELECT
    merchants.mid AS merchant_id,
    merchants.name AS merchant_name,
    merchants.mid AS mid
  FROM main.merchants AS merchants
)
SELECT
  _t0.merchant_id AS merchant_id,
  _t0.merchant_name AS merchant_name
FROM _t0 AS _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1 AS _t1
    WHERE
      _t0.mid = _t1.merchant_id
  )
