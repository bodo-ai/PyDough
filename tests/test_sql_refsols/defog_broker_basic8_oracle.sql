SELECT
  sbcustcountry AS country,
  COUNT(*) AS num_customers
FROM MAIN.SBCUSTOMER
GROUP BY
  sbcustcountry
ORDER BY
  2 DESC NULLS LAST
FETCH FIRST 5 ROWS ONLY
