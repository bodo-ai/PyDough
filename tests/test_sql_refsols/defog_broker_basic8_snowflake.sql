SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM MAIN.SBCUSTOMER
GROUP BY
  sbcustcountry
ORDER BY
  NUM_CUSTOMERS DESC NULLS LAST
LIMIT 5
