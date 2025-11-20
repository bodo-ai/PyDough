WITH _s1 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.INCIDENTS
  GROUP BY
    1
), _s3 AS (
  SELECT
    DEVICES.de_product_id,
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_nrows
  FROM main.DEVICES AS DEVICES
  LEFT JOIN _s1 AS _s1
    ON DEVICES.de_id = _s1.in_device_id
  GROUP BY
    1
)
SELECT
  PRODUCTS.pr_name AS product,
  PRODUCTS.pr_brand AS product_brand,
  PRODUCTS.pr_type AS product_type,
  ROUND(COALESCE(_s3.sum_nrows, 0) / _s3.n_rows, 2) AS ir
FROM main.PRODUCTS AS PRODUCTS
JOIN _s3 AS _s3
  ON PRODUCTS.pr_id = _s3.de_product_id
ORDER BY
  4 DESC
LIMIT 5
