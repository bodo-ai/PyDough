WITH _s1 AS (
  SELECT
    merchant_id
  FROM main.coupons
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name
FROM main.merchants AS merchants
ANTI JOIN _s1 AS _s1
  ON _s1.merchant_id = merchants.mid
