WITH _s1 AS (
  SELECT
    "CDSCode",
    "Percent (%) Eligible FRPM (K-12)"
  FROM main.frpm
)
SELECT
  _s3."Percent (%) Eligible FRPM (K-12)" AS "percent_eligible_frpm_k_12",
  IFF(_s1."Percent (%) Eligible FRPM (K-12)" >= 0.75, 'High', 'Medium-Low') AS frpm_category
FROM main.schools AS schools
LEFT JOIN _s1 AS _s1
  ON _s1."CDSCode" = schools."CDSCode"
LEFT JOIN _s1 AS _s3
  ON _s3."CDSCode" = schools."CDSCode"
ORDER BY
  1 DESC NULLS LAST
LIMIT 5
