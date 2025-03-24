SELECT
  mid AS merchant_id,
  name AS merchant_name
FROM (
  SELECT
    mid,
    name
  FROM main.merchants
)
ANTI JOIN (
  SELECT
    merchant_id
  FROM main.coupons
)
  ON mid = merchant_id
