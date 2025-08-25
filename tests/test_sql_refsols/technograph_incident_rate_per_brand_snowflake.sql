WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.incidents
  GROUP BY
    2
)
SELECT
  products.pr_brand AS brand,
  ROUND(COALESCE(SUM(_s3.n_rows), 0) / COUNT(*), 2) AS ir
FROM main.devices AS devices
JOIN main.products AS products
  ON devices.de_product_id = products.pr_id
LEFT JOIN _s3 AS _s3
  ON _s3.in_device_id = devices.de_id
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
