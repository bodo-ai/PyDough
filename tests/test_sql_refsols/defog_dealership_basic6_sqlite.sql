WITH _t1 AS (
  SELECT
    SUM(_s0.sale_price) AS agg_0,
    COUNT(DISTINCT _s0.customer_id) AS agg_1,
    _s1.state
  FROM main.sales AS _s0
  JOIN main.customers AS _s1
    ON _s0.customer_id = _s1._id
  GROUP BY
    _s1.state
)
SELECT
  state,
  agg_1 AS unique_customers,
  COALESCE(agg_0, 0) AS total_revenue
FROM _t1
ORDER BY
  total_revenue DESC
LIMIT 5
