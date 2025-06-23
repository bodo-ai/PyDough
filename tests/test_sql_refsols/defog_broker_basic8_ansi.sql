WITH _t0 AS (
  SELECT
    COUNT(*) AS num_customers,
    sbcustcountry
  FROM main.sbcustomer
  GROUP BY
    sbcustcountry
)
SELECT
  sbcustcountry AS country,
  num_customers
FROM _t0
ORDER BY
  num_customers DESC
LIMIT 5
