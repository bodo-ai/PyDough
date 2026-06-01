SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM mongo.defog.sbcustomer
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 5
