SELECT
  ARBITRARY(merchants.name) AS merchant_name,
  COUNT(*) AS total_coupons
FROM postgres.main.merchants AS merchants
JOIN postgres.main.coupons AS coupons
  ON coupons.merchant_id = merchants.mid
WHERE
  LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
GROUP BY
  coupons.merchant_id
