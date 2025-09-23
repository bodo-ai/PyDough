WITH _s3 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    1
)
SELECT
  products.pr_brand AS brand,
  ROUND(CAST(CAST(SUM(_s3.n_rows) AS DOUBLE PRECISION) / COUNT(*) AS DECIMAL), 2) AS ir
FROM main.devices AS devices
JOIN main.products AS products
  ON devices.de_product_id = products.pr_id
JOIN _s3 AS _s3
  ON _s3.in_device_id = devices.de_id
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
