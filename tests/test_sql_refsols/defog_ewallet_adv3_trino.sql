SELECT
  ARBITRARY(merchants.name) AS merchant_name,
  COUNT(*) AS total_coupons
FROM mongo.defog.merchants AS merchants
JOIN cassandra.defog.coupons AS coupons
  ON coupons.merchant_id = merchants.mid
WHERE
  LOWER(merchants.category) LIKE '%retail%' AND merchants.status = 'active'
GROUP BY
  coupons.merchant_id
