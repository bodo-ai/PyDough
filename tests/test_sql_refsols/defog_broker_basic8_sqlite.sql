WITH _t0 AS (
  SELECT
    sbcustcountry AS country,
    COUNT(*) AS num_customers
  FROM main.sbcustomer
  GROUP BY
    sbcustcountry
)
SELECT
  country,
  num_customers
FROM _t0
ORDER BY
  num_customers DESC
LIMIT 5
