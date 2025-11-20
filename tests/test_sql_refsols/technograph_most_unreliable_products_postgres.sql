WITH _s1 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    1
), _s3 AS (
  SELECT
    devices.de_product_id,
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_n_rows
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1.in_device_id = devices.de_id
  GROUP BY
    1
)
SELECT
  products.pr_name AS product,
  products.pr_brand AS product_brand,
  products.pr_type AS product_type,
  ROUND(
    CAST(CAST(COALESCE(_s3.sum_n_rows, 0) AS DOUBLE PRECISION) / _s3.n_rows AS DECIMAL),
    2
  ) AS ir
FROM main.products AS products
JOIN _s3 AS _s3
  ON _s3.de_product_id = products.pr_id
ORDER BY
  4 DESC NULLS LAST
LIMIT 5
