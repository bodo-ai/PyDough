WITH _t2 AS (
  SELECT
    SUM(sales.sale_price) AS agg_0,
    customers.state AS customer_state,
    CASE
      WHEN EXTRACT(MONTH FROM sales.sale_date) <= 3
      THEN '2023-01-01'
      ELSE CASE
        WHEN EXTRACT(MONTH FROM sales.sale_date) <= 6
        THEN '2023-04-01'
        ELSE CASE
          WHEN EXTRACT(MONTH FROM sales.sale_date) <= 9
          THEN '2023-07-01'
          ELSE '2023-10-01'
        END
      END
    END AS quarter
  FROM main.sales AS sales
  LEFT JOIN main.customers AS customers
    ON customers._id = sales.customer_id
  WHERE
    EXTRACT(YEAR FROM sales.sale_date) = 2023
  GROUP BY
    customers.state,
    CASE
      WHEN EXTRACT(MONTH FROM sales.sale_date) <= 3
      THEN '2023-01-01'
      ELSE CASE
        WHEN EXTRACT(MONTH FROM sales.sale_date) <= 6
        THEN '2023-04-01'
        ELSE CASE
          WHEN EXTRACT(MONTH FROM sales.sale_date) <= 9
          THEN '2023-07-01'
          ELSE '2023-10-01'
        END
      END
    END
)
SELECT
  quarter,
  customer_state,
  COALESCE(agg_0, 0) AS total_sales
FROM _t2
WHERE
  NOT agg_0 IS NULL AND agg_0 > 0
ORDER BY
  quarter,
  customer_state
