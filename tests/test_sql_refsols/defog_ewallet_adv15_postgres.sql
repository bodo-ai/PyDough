WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    coupons.merchant_id
  FROM main.coupons AS coupons
  JOIN main.merchants AS merchants
    ON CAST(EXTRACT(YEAR FROM CAST(AGE(CAST(coupons.created_at AS TIMESTAMP), CAST(merchants.created_at AS TIMESTAMP)) AS TIMESTAMP)) * 12 + EXTRACT(MONTH FROM CAST(AGE(CAST(coupons.created_at AS TIMESTAMP), CAST(merchants.created_at AS TIMESTAMP)) AS TIMESTAMP)) AS BIGINT) = 0
    AND coupons.merchant_id = merchants.mid
  GROUP BY
    coupons.merchant_id
)
SELECT
  merchants.mid AS merchant_id,
  merchants.name AS merchant_name,
  COALESCE(_s3.n_rows, 0) AS coupons_per_merchant
FROM main.merchants AS merchants
LEFT JOIN _s3 AS _s3
  ON _s3.merchant_id = merchants.mid
ORDER BY
  COALESCE(_s3.n_rows, 0) DESC NULLS LAST
LIMIT 1
