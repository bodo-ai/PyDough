SELECT
  LAG(R_NAME, 2) OVER (ORDER BY R_NAME) AS two_preceding,
  LAG(R_NAME, 1) OVER (ORDER BY R_NAME) AS one_preceding,
  R_NAME AS current_region,
  LEAD(R_NAME, 1) OVER (ORDER BY R_NAME) AS one_following,
  LEAD(R_NAME, 2) OVER (ORDER BY R_NAME) AS two_following
FROM TPCH.REGION
ORDER BY
  3 NULLS FIRST
