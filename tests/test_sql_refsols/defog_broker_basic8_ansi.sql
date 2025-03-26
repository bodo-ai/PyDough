WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  GROUP BY
    sbcustomer.sbcustcountry
), _t0 AS (
  SELECT
    _t2.country AS country,
    COALESCE(_t2.agg_0, 0) AS num_customers,
    COALESCE(_t2.agg_0, 0) AS ordering_1
  FROM _t2 AS _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  _t0.country AS country,
  _t0.num_customers AS num_customers
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
