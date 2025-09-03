SELECT
  sbcustcountry AS country,
  100 * (
    CAST(COALESCE(SUM(CASE WHEN sbcuststatus = 'active' THEN 1 ELSE 0 END), 0) AS DOUBLE PRECISION) / COUNT(*)
  ) AS ar
FROM main.sbcustomer
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  1
