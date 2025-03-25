SELECT
  mid AS merchant_id,
  name AS merchant_name
FROM (
  SELECT
    mid,
    name
  FROM main.merchants
) AS _table_alias_0
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        merchant_id
      FROM main.coupons
    ) AS _table_alias_1
    WHERE
      mid = merchant_id
  )
