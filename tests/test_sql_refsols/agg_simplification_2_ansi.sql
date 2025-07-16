SELECT
  sbcuststate AS state,
  COUNT(DISTINCT sbcustcity) AS a1,
  COALESCE(COUNT(*), 0) AS a2,
  COALESCE(COUNT(CASE WHEN LOWER(sbcustname) LIKE 'j%' THEN sbcustname ELSE NULL END), 0) AS a3,
  COALESCE(COALESCE(SUM(CAST(sbcustpostalcode AS BIGINT)), 0), 0) AS a4,
  MIN(sbcustphone) AS a5,
  MAX(sbcustphone) AS a6,
  ANY_VALUE(LOWER(sbcuststate)) AS a7,
  ANY_VALUE(LOWER(sbcuststate)) AS a8,
  ANY_VALUE(LOWER(sbcuststate)) AS a9
FROM main.sbcustomer
GROUP BY
  sbcuststate
ORDER BY
  sbcuststate
