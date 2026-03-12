SELECT
  mid AS merchant_id,
  name AS merchant_name
FROM ewallet.merchants
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM ewallet.coupons
    WHERE
      merchant_id = merchants.mid
  )
