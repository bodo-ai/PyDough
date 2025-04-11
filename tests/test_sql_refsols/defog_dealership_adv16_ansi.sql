WITH _t1 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
), _t0_2 AS (
  SELECT
    salespersons._id,
    salespersons.first_name,
    salespersons.last_name,
    COALESCE(_t1.agg_0, 0) AS ordering_1,
    COALESCE(_t1.agg_0, 0) AS total
  FROM main.salespersons AS salespersons
  LEFT JOIN _t1 AS _t1
    ON _t1.salesperson_id = salespersons._id
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  _id,
  first_name,
  last_name,
  total
FROM _t0_2
ORDER BY
  ordering_1 DESC
