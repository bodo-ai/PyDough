WITH _t AS (
  SELECT
    pr_name,
    pr_prodcost,
    pr_release,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(STRFTIME('%Y', pr_release) AS INTEGER)
      ORDER BY pr_prodcost DESC
    ) AS _w
  FROM main.products
)
SELECT
  CAST(STRFTIME('%Y', pr_release) AS INTEGER) AS release_year,
  pr_name AS name,
  pr_prodcost AS production_cost
FROM _t
WHERE
  _w = 1
