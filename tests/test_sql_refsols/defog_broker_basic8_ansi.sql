WITH _t2 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustcountry AS country
  FROM main.sbcustomer
  GROUP BY
    sbcustcountry
), _t0 AS (
  SELECT
    country,
    COALESCE(agg_0, 0) AS num_customers,
    COALESCE(agg_0, 0) AS ordering_1
  FROM _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  country,
  num_customers
FROM _t0
ORDER BY
  ordering_1 DESC
