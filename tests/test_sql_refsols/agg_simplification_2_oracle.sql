SELECT
  sbcuststate AS state,
  COUNT(DISTINCT sbcustcity) AS a1,
  COUNT(*) AS a2,
  COUNT(CASE WHEN LOWER(sbcustname) LIKE 'j%' THEN sbcustname ELSE NULL END) AS a3,
  COALESCE(SUM(CAST(sbcustpostalcode AS INT)), 0) AS a4,
  MIN(sbcustphone) AS a5,
  MAX(sbcustphone) AS a6,
  ANY_VALUE(LOWER(sbcuststate)) AS a7,
  ANY_VALUE(LOWER(sbcuststate)) AS a8,
  ANY_VALUE(LOWER(sbcuststate)) AS a9
FROM MAIN.SBCUSTOMER
GROUP BY
  sbcuststate
ORDER BY
  1 NULLS FIRST
