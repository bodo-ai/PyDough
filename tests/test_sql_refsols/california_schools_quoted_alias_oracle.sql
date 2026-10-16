WITH "_s1" AS (
  SELECT
    "CDSCode",
    "Percent (%) Eligible FRPM (K-12)"
  FROM MAIN.FRPM
)
SELECT
  "_s3"."Percent (%) Eligible FRPM (K-12)" AS "percent_eligible_frpm_k_12",
  CASE
    WHEN "_s1"."Percent (%) Eligible FRPM (K-12)" >= 0.75
    THEN 'High'
    ELSE 'Medium-Low'
  END AS frpm_category
FROM MAIN.SCHOOLS SCHOOLS
LEFT JOIN "_s1" "_s1"
  ON SCHOOLS."CDSCode" = "_s1"."CDSCode"
LEFT JOIN "_s1" "_s3"
  ON SCHOOLS."CDSCode" = "_s3"."CDSCode"
ORDER BY
  1 DESC NULLS LAST
FETCH FIRST 5 ROWS ONLY
