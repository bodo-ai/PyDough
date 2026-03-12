SELECT
  mid AS merchant_id,
  name AS merchant_name
FROM main.merchants
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.coupons
    WHERE
      merchant_id = merchants.mid
  )
