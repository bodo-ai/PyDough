SELECT
  mid AS merchant_id,
  name AS merchant_name
FROM (
  SELECT
    mid,
    name
  FROM main.merchants
)
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        merchant_id
      FROM main.coupons
    )
    WHERE
      mid = merchant_id
  )
