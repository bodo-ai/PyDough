SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM broker.sbcustomer
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
