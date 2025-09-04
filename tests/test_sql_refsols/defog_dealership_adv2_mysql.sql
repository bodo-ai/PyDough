WITH _s1 AS (
  SELECT
    COUNT(*) AS num_sales,
    salesperson_id
  FROM main.sales
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sale_date) <= 30
  GROUP BY
    2
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name,
  _s1.num_sales
FROM main.salespersons AS salespersons
JOIN _s1 AS _s1
  ON _s1.salesperson_id = salespersons._id
ORDER BY
  4 DESC,
  1
