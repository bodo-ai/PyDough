WITH _s3 AS (
  SELECT
    in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    in_device_id
), _t0 AS (
  SELECT
    products.pr_brand AS brand,
    COUNT(*) AS n_rows,
    SUM(COALESCE(_s3.n_rows, 0)) AS sum_n_incidents
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  LEFT JOIN _s3 AS _s3
    ON _s3.device_id = devices.de_id
  GROUP BY
    products.pr_brand
)
SELECT
  brand,
  ROUND(CAST(COALESCE(sum_n_incidents, 0) AS REAL) / n_rows, 2) AS ir
FROM _t0
ORDER BY
  brand
