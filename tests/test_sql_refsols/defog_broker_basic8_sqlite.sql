WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustcountry AS country
  FROM main.sbcustomer
  GROUP BY
    sbcustcountry
)
SELECT
  country,
  COALESCE(agg_0, 0) AS num_customers
FROM _t1
ORDER BY
  num_customers DESC
LIMIT 5
