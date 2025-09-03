SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM main.sbcustomer
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
