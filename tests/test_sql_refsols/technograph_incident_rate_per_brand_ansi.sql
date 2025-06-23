WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(COALESCE(_s3.n_rows, 0)) AS sum_n_incidents,
    products.pr_brand
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  LEFT JOIN _s3 AS _s3
    ON _s3.in_device_id = devices.de_id
  GROUP BY
    products.pr_brand
)
SELECT
  pr_brand AS brand,
  ROUND(COALESCE(sum_n_incidents, 0) / n_rows, 2) AS ir
FROM _t1
ORDER BY
  pr_brand
