WITH _t0 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    COUNT(*) AS agg_1,
    salesperson_id
  FROM main.sales
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sale_date, DAY) <= 30
  GROUP BY
    salesperson_id
)
SELECT
  _s0.first_name,
  _s0.last_name,
  _t0.agg_1 AS total_sales,
  COALESCE(_t0.agg_0, 0) AS total_revenue
FROM main.salespersons AS _s0
JOIN _t0 AS _t0
  ON _s0._id = _t0.salesperson_id
ORDER BY
  total_sales DESC
LIMIT 5
