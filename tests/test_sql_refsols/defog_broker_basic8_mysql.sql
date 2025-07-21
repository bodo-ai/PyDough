SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM main.sbCustomer
GROUP BY
  sbcustcountry
ORDER BY
  num_customers DESC
LIMIT 5
