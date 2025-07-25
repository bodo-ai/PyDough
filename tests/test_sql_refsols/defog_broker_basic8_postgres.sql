SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM main.sbcustomer
GROUP BY
  sbcustcountry
ORDER BY
  num_customers DESC NULLS LAST
LIMIT 5
