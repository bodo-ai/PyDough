SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name
FROM main.merchants AS merchants
JOIN main.coupons AS coupons
  ON coupons.merchant_id = merchants.mid
