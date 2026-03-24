SELECT
  sbcuststate AS state,
  COUNT(DISTINCT sbcustcity) AS a1,
  COUNT(*) AS a2,
  COUNT(CASE WHEN STARTS_WITH(LOWER(sbcustname), 'j') THEN sbcustname ELSE NULL END) AS a3,
  COALESCE(SUM(CAST(CAST(sbcustpostalcode AS DOUBLE) AS BIGINT)), 0) AS a4,
  MIN(sbcustphone) AS a5,
  MAX(sbcustphone) AS a6,
  ARBITRARY(LOWER(sbcuststate)) AS a7,
  ARBITRARY(LOWER(sbcuststate)) AS a8,
  ARBITRARY(LOWER(sbcuststate)) AS a9
FROM main.sbcustomer
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
