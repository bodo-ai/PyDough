SELECT
  sbcustcountry AS cust_country,
  COUNT(*) AS TAC
FROM MAIN.SBCUSTOMER
WHERE
  sbcustjoindate >= CAST('2023-01-01' AS DATE)
GROUP BY
  1
