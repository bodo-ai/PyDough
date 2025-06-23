WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(COALESCE(_s3.n_rows, 0)) AS sum_n_incidents,
    devices.de_product_id
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  LEFT JOIN _s3 AS _s3
    ON _s3.in_device_id = devices.de_id
  GROUP BY
    devices.de_product_id
)
SELECT
  products.pr_name AS product,
  products.pr_brand AS product_brand,
  products.pr_type AS product_type,
  ROUND(COALESCE(_t0.sum_n_incidents, 0) / _t0.n_rows, 2) AS ir
FROM main.products AS products
JOIN _t0 AS _t0
  ON _t0.de_product_id = products.pr_id
ORDER BY
  ir DESC
LIMIT 5
