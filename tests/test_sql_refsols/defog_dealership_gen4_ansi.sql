WITH _s0 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    customer_id,
    DATE_TRUNC('QUARTER', CAST(sale_date AS TIMESTAMP)) AS quarter
  FROM main.sales
  WHERE
    EXTRACT(YEAR FROM CAST(sale_date AS DATETIME)) = 2023
  GROUP BY
    customer_id,
    DATE_TRUNC('QUARTER', CAST(sale_date AS TIMESTAMP))
), _t1 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    customers.state AS customer_state,
    _s0.quarter
  FROM _s0 AS _s0
  JOIN main.customers AS customers
    ON _s0.customer_id = customers._id
  GROUP BY
    customers.state,
    _s0.quarter
)
SELECT
  quarter,
  customer_state,
  COALESCE(agg_0, 0) AS total_sales
FROM _t1
WHERE
  NOT agg_0 IS NULL AND agg_0 > 0
ORDER BY
  quarter,
  customer_state
