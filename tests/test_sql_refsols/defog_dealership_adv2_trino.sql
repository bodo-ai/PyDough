WITH _s1 AS (
  SELECT
    salesperson_id,
    COUNT(*) AS n_rows
  FROM postgres.main.sales
  WHERE
    DATE_DIFF(
      'DAY',
      CAST(DATE_TRUNC('DAY', CAST(sale_date AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) AS TIMESTAMP)
    ) <= 30
  GROUP BY
    1
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name,
  _s1.n_rows AS num_sales
FROM postgres.main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  4 DESC,
  1 NULLS FIRST
