WITH _s2 AS (
  SELECT
    devices.de_product_id,
    MAX(products.pr_name) AS anything_pr_name,
    COUNT(*) AS n_rows
  FROM main.products AS products
  JOIN main.devices AS devices
    ON devices.de_product_id = products.pr_id
    AND devices.de_product_id IN (618070, 960138, 143712)
  WHERE
    products.pr_id IN (618070, 960138, 143712)
  GROUP BY
    1
), _s3 AS (
  SELECT
    in_product_id,
    COUNT(DISTINCT in_device_id) AS ndistinct_in_device_id
  FROM main.incidents
  GROUP BY
    1
)
SELECT
  _s2.anything_pr_name AS product_name,
  ROUND(CAST(COALESCE(_s3.ndistinct_in_device_id, 0) AS REAL) / _s2.n_rows, 2) AS lifetime_ir
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.de_product_id = _s3.in_product_id
ORDER BY
  1
