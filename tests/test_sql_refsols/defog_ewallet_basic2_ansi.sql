SELECT
  _s0.mid AS merchant_id,
  _s0.name AS merchant_name
FROM main.merchants AS _s0
JOIN main.coupons AS _s1
  ON _s0.mid = _s1.merchant_id
