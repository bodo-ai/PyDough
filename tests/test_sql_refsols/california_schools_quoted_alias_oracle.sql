WITH "_S1" AS (
  SELECT
    "CDSCode",
    "Percent (%) Eligible FRPM (K-12)"
  FROM MAIN.FRPM
)
SELECT
  "_S3"."Percent (%) Eligible FRPM (K-12)" AS "percent_eligible_frpm_k_12",
  CASE
    WHEN "_S1"."Percent (%) Eligible FRPM (K-12)" >= 0.75
    THEN 'High'
    ELSE 'Medium-Low'
  END AS frpm_category
FROM MAIN.SCHOOLS SCHOOLS
LEFT JOIN "_S1" "_S1"
  ON SCHOOLS."CDSCode" = "_S1"."CDSCode"
LEFT JOIN "_S1" "_S3"
  ON SCHOOLS."CDSCode" = "_S3"."CDSCode"
ORDER BY
  1 DESC NULLS LAST
FETCH FIRST 5 ROWS ONLY
