WITH _s2 AS (
  SELECT
    DEVICES.de_product_id,
    ANY_VALUE(PRODUCTS.pr_name) AS anything_pr_name,
    COUNT(*) AS n_rows
  FROM main.PRODUCTS AS PRODUCTS
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_product_id = PRODUCTS.pr_id
  WHERE
    PRODUCTS.pr_id IN (618070, 960138, 143712)
  GROUP BY
    1
), _s3 AS (
  SELECT
    in_product_id,
    COUNT(DISTINCT in_device_id) AS ndistinct_in_device_id
  FROM main.INCIDENTS
  GROUP BY
    1
)
SELECT
  _s2.anything_pr_name COLLATE utf8mb4_bin AS product_name,
  ROUND(COALESCE(_s3.ndistinct_in_device_id, 0) / _s2.n_rows, 2) AS lifetime_ir
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.de_product_id = _s3.in_product_id
ORDER BY
  1
