SELECT
  sbcuststate AS state,
  COUNT(DISTINCT sbcustcity) AS a1,
  COUNT(*) AS a2,
  COUNT(CASE WHEN STARTSWITH(LOWER(sbcustname), 'j') THEN sbcustname ELSE NULL END) AS a3,
  COALESCE(SUM(CAST(sbcustpostalcode AS BIGINT)), 0) AS a4,
  MIN(sbcustphone) AS a5,
  MAX(sbcustphone) AS a6,
  ANY_VALUE(LOWER(sbcuststate)) AS a7,
  ANY_VALUE(LOWER(sbcuststate)) AS a8,
  ANY_VALUE(LOWER(sbcuststate)) AS a9
FROM MAIN.SBCUSTOMER
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
