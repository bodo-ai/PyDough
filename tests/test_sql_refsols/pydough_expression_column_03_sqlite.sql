WITH _s1 AS (
  SELECT
    de_product_id,
    COUNT(*) AS n_rows
  FROM main.devices
  GROUP BY
    1
)
SELECT
  products.pr_id AS _id,
  products.pr_name AS name,
  products.pr_type AS category,
  products.pr_brand AS brand,
  products.pr_prodcost AS production_cost,
  products.pr_release AS release_date,
  CONCAT_WS('', SUBSTRING(products.pr_brand, 1, 1), UPPER(SUBSTRING(products.pr_type, 1, 1))) AS brand_cat,
  CAST(STRFTIME('%Y', products.pr_release) AS INTEGER) AS release_year,
  CONCAT_WS(
    '',
    CAST(CAST(STRFTIME('%Y', products.pr_release) AS INTEGER) AS TEXT),
    'Q',
    CAST(CASE
      WHEN CAST(STRFTIME('%m', products.pr_release) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', products.pr_release) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', products.pr_release) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', products.pr_release) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', products.pr_release) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', products.pr_release) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', products.pr_release) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', products.pr_release) AS INTEGER) >= 10
      THEN 4
    END AS TEXT)
  ) AS release_fiscal_quarter,
  _s1.n_rows AS n_devices
FROM main.products AS products
JOIN _s1 AS _s1
  ON _s1.de_product_id = products.pr_id
ORDER BY
  1
LIMIT 3
