WITH _s2 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    customer_id,
    DATE_TRUNC('QUARTER', CAST(sale_date AS TIMESTAMP)) AS quarter
  FROM main.sales
  WHERE
    EXTRACT(YEAR FROM sale_date) = 2023
  GROUP BY
    customer_id,
    DATE_TRUNC('QUARTER', CAST(sale_date AS TIMESTAMP))
), _t1 AS (
  SELECT
    SUM(_s2.agg_0) AS agg_0,
    _s1.state AS customer_state,
    _s2.quarter
  FROM _s2 AS _s2
  JOIN main.customers AS _s1
    ON _s1._id = _s2.customer_id
  GROUP BY
    _s1.state,
    _s2.quarter
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
