WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.INCIDENTS
  GROUP BY
    in_device_id
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(_s3.n_rows) AS sum_n_incidents,
    DEVICES.de_product_id
  FROM main.DEVICES AS DEVICES
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id
  LEFT JOIN _s3 AS _s3
    ON DEVICES.de_id = _s3.in_device_id
  GROUP BY
    DEVICES.de_product_id
)
SELECT
  PRODUCTS.pr_name AS product,
  PRODUCTS.pr_brand AS product_brand,
  PRODUCTS.pr_type AS product_type,
  ROUND(COALESCE(_s5.sum_n_incidents, 0) / _s5.n_rows, 2) AS ir
FROM main.PRODUCTS AS PRODUCTS
JOIN _s5 AS _s5
  ON PRODUCTS.pr_id = _s5.de_product_id
ORDER BY
  ROUND(COALESCE(_s5.sum_n_incidents, 0) / _s5.n_rows, 2) DESC
LIMIT 5
