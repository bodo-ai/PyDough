WITH _t0 AS (
  SELECT
    r_name AS current,
    LEAD(r_name, 1) OVER (ORDER BY r_name) AS one_following,
    LAG(r_name, 1) OVER (ORDER BY r_name) AS one_preceding,
    LEAD(r_name, 2) OVER (ORDER BY r_name) AS two_following,
    LAG(r_name, 2) OVER (ORDER BY r_name) AS two_preceding
  FROM tpch.region
)
SELECT
  two_preceding,
  one_preceding,
  current,
  one_following,
  two_following
FROM _t0
ORDER BY
  current
