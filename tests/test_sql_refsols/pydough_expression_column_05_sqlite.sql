WITH _s2 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    1
)
SELECT
  products.pr_name AS product_name,
  SUM(_s2.n_rows) AS n_incidents
FROM _s2 AS _s2
JOIN main.devices AS devices
  ON _s2.in_device_id = devices.de_id
JOIN main.products AS products
  ON devices.de_product_id = products.pr_id
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 3
