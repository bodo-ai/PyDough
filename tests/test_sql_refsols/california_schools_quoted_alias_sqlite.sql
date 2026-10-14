WITH _s1 AS (
  SELECT
    "cdscode",
    "percent (%) eligible frpm (k-12)"
  FROM main.frpm
)
SELECT
  _s3."percent (%) eligible frpm (k-12)" AS "percent_eligible_frpm_k_12",
  IIF(_s1."percent (%) eligible frpm (k-12)" >= 0.75, 'High', 'Medium-Low') AS frpm_category
FROM main.schools AS schools
LEFT JOIN _s1 AS _s1
  ON _s1."cdscode" = schools."cdscode"
LEFT JOIN _s1 AS _s3
  ON _s3."cdscode" = schools."cdscode"
ORDER BY
  1 DESC
LIMIT 5
