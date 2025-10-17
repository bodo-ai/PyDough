SELECT
  MAX(merchants.name) AS merchant_name,
  COUNT(*) AS total_coupons
FROM main.merchants AS merchants
JOIN main.coupons AS coupons
  ON coupons.merchant_id = merchants.mid
WHERE
  LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
GROUP BY
  coupons.merchant_id
