WITH "_S3" AS (
  SELECT
    drug_id AS DRUG_ID,
    AVG(
      tot_drug_amt / CASE
        WHEN (
          CAST(end_dt AS DATE) - CAST(start_dt AS DATE)
        ) <> 0
        THEN CAST(end_dt AS DATE) - CAST(start_dt AS DATE)
        ELSE NULL
      END
    ) AS AVG_DDD
  FROM MAIN.TREATMENTS
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    drug_id
)
SELECT
  DRUGS.drug_name,
  "_S3".AVG_DDD AS avg_ddd
FROM MAIN.DRUGS DRUGS
JOIN MAIN.TREATMENTS TREATMENTS
  ON DRUGS.drug_id = TREATMENTS.drug_id AND NOT TREATMENTS.end_dt IS NULL
LEFT JOIN "_S3" "_S3"
  ON DRUGS.drug_id = "_S3".DRUG_ID
