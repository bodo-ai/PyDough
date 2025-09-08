WITH _s1 AS (
  SELECT
    salesperson_id,
    COUNT(*) AS n_rows
  FROM main.sales
  WHERE
    DATEDIFF(DAY, CAST(sale_date AS DATETIME), CURRENT_TIMESTAMP()) <= 30
  GROUP BY
    1
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name,
  _s1.n_rows AS num_sales
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  4 DESC NULLS LAST,
  1 NULLS FIRST
