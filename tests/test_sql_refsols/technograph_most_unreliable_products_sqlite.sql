WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.incidents
  GROUP BY
    2
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_n_rows,
    devices.de_product_id
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1.in_device_id = devices.de_id
  GROUP BY
    3
)
SELECT
  products.pr_name AS product,
  products.pr_brand AS product_brand,
  products.pr_type AS product_type,
  ROUND(CAST(COALESCE(_s3.sum_n_rows, 0) AS REAL) / _s3.n_rows, 2) AS ir
FROM main.products AS products
JOIN _s3 AS _s3
  ON _s3.de_product_id = products.pr_id
ORDER BY
  4 DESC
LIMIT 5
