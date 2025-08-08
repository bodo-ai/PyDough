WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    salesperson_id
  FROM main.sales
  WHERE
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - CAST(sale_date AS TIMESTAMP)) / 86400 <= 30
  GROUP BY
    salesperson_id
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
  _s1.n_rows DESC NULLS LAST
