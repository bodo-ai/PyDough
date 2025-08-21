SELECT
  sbcustcountry AS cust_country,
  COUNT(*) AS TAC
FROM main.sbcustomer
WHERE
  sbcustjoindate >= '2023-01-01'
GROUP BY
  1
