WITH _t1 AS (
  SELECT
    merchant_id AS merchant_id
  FROM main.coupons
), _t0 AS (
  SELECT
    mid AS mid,
    name AS name
  FROM main.merchants
)
SELECT
  mid AS merchant_id,
  name AS merchant_name
FROM _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      mid = merchant_id
  )
