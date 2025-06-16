WITH _s1 AS (
  SELECT
    coupons.merchant_id AS merchant_id
  FROM main.coupons AS coupons
), _s0 AS (
  SELECT
    merchants.mid AS mid,
    merchants.name AS name
  FROM main.merchants AS merchants
)
SELECT
  _s0.mid AS merchant_id,
  _s0.name AS merchant_name
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.mid = _s1.merchant_id
  )
