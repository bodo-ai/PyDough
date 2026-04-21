SELECT
  sbcustcountry AS cust_country,
  COUNT(*) AS TAC
FROM mongo.defog.sbcustomer
WHERE
  sbcustjoindate >= CAST('2023-01-01' AS DATE)
GROUP BY
  1
