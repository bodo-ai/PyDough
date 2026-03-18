SELECT
  sbcustcountry AS country,
  100 * (
    CAST(COUNT_IF(sbcuststatus = 'active') AS DOUBLE) / COUNT(*)
  ) AS ar
FROM postgres.main.sbcustomer
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  1
