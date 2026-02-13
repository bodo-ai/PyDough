SELECT
  sbcuststate AS state,
  COUNT(DISTINCT sbcustcity) AS a1,
  COUNT(*) AS a2,
  COUNT(CASE WHEN LOWER(sbcustname) LIKE 'j%' THEN sbcustname ELSE NULL END) AS a3,
  COALESCE(SUM(CAST(sbcustpostalcode AS INTEGER)), 0) AS a4,
  MIN(sbcustphone) AS a5,
  MAX(sbcustphone) AS a6,
  MAX(LOWER(sbcuststate)) AS a7,
  MAX(LOWER(sbcuststate)) AS a8,
  MAX(LOWER(sbcuststate)) AS a9
FROM main.sbcustomer
GROUP BY
  1
ORDER BY
  1
