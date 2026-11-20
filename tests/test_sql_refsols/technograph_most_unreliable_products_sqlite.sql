WITH _s1 AS (
  SELECT
    de_id,
    de_product_id
  FROM main.devices
), _s7 AS (
  SELECT
    _s2.de_id,
    _s2.de_product_id
  FROM _s1 AS _s2
  JOIN main.incidents AS incidents
    ON _s2.de_id = incidents.in_device_id
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
), _t1 AS (
  SELECT
    products.pr_id,
    MAX(products.pr_brand) AS anything_pr_brand,
    MAX(products.pr_name) AS anything_pr_name,
    MAX(products.pr_type) AS anything_pr_type,
    COUNT(_s7.de_product_id) AS count_de_product_id
  FROM main.products AS products
  JOIN _s1 AS _s1
    ON _s1.de_product_id = products.pr_id
  LEFT JOIN _s7 AS _s7
    ON _s1.de_id = _s7.de_id AND _s7.de_product_id = products.pr_id
  GROUP BY
    _s1.de_id,
    1
)
SELECT
  MAX(anything_pr_name) AS product,
  MAX(anything_pr_brand) AS product_brand,
  MAX(anything_pr_type) AS product_type,
  ROUND(CAST(COALESCE(SUM(count_de_product_id), 0) AS REAL) / COUNT(*), 2) AS ir
FROM _t1
GROUP BY
  pr_id
ORDER BY
  4 DESC
LIMIT 5
