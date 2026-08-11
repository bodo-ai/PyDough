WITH _s1 AS (
  SELECT
    salesperson_id,
    COUNT(*) AS n_rows
  FROM defog.dealership.sales
  WHERE
    DATEDIFF(DAY, CAST(sale_date AS DATE), CAST(CURRENT_TIMESTAMP() AS DATE)) <= 30
  GROUP BY
    1
)
SELECT
  salespersons.id AS _id,
  salespersons.first_name,
  salespersons.last_name,
  _s1.n_rows AS num_sales
FROM defog.dealership.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons.id
ORDER BY
  4 DESC,
  1
