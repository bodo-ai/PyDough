SELECT
  LAG(r_name, 2) OVER (ORDER BY r_name NULLS LAST) AS two_preceding,
  LAG(r_name, 1) OVER (ORDER BY r_name NULLS LAST) AS one_preceding,
  r_name AS current,
  LEAD(r_name, 1) OVER (ORDER BY r_name NULLS LAST) AS one_following,
  LEAD(r_name, 2) OVER (ORDER BY r_name NULLS LAST) AS two_following
FROM tpch.region
ORDER BY
  3
