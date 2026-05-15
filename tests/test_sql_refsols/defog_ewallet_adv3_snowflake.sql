SELECT
  ANY_VALUE(merchants.name) AS merchant_name,
  COUNT(*) AS total_coupons
FROM ewallet.merchants AS merchants
JOIN ewallet.coupons AS coupons
  ON coupons.merchant_id = merchants.mid
WHERE
  CONTAINS(LOWER(merchants.category), 'retail') AND merchants.status = 'active'
GROUP BY
  coupons.merchant_id
