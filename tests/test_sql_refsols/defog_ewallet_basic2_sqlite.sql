SELECT
  _s0.mid AS merchant_id,
  _s0.name AS merchant_name
FROM main.merchants AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.coupons AS _s1
    WHERE
      _s0.mid = _s1.merchant_id
  )
