SELECT
  sbcustcountry AS country,
  100 * (
    CAST(COUNT_IF(sbcuststatus = 'active') AS DOUBLE) / COUNT(*)
  ) AS ar
FROM mongo.defog.sbcustomer
WHERE
  sbcustjoindate <= CAST('2022-12-31' AS DATE)
  AND sbcustjoindate >= CAST('2022-01-01' AS DATE)
GROUP BY
  1
