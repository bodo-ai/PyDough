SELECT
  sbcustcountry AS cust_country,
  COUNT() AS TAC
FROM main.sbcustomer
WHERE
  sbcustjoindate >= CAST('2023-01-01' AS DATE)
GROUP BY
  sbcustcountry
