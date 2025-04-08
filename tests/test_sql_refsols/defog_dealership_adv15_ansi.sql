WITH _t1 AS (
  SELECT
    AVG(sale_price) AS agg_0,
    salesperson_id
  FROM main.sales
  GROUP BY
    salesperson_id
), _t0_2 AS (
  SELECT
    _t1.agg_0 AS asp,
    salespersons.first_name,
    salespersons.last_name,
    _t1.agg_0 AS ordering_1
  FROM main.salespersons AS salespersons
  LEFT JOIN _t1 AS _t1
    ON _t1.salesperson_id = salespersons._id
  ORDER BY
    ordering_1 DESC
  LIMIT 3
)
SELECT
  first_name,
  last_name,
  asp AS ASP
FROM _t0_2
ORDER BY
  ordering_1 DESC
