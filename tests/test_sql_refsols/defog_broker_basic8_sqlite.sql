SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM main.sbcustomer
GROUP BY
  1
ORDER BY
  num_customers DESC
LIMIT 5
