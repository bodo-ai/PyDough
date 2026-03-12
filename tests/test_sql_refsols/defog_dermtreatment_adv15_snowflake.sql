WITH _s3 AS (
  SELECT
    drug_id,
    AVG(
      tot_drug_amt / NULLIF(DATEDIFF(DAY, CAST(start_dt AS DATETIME), CAST(end_dt AS DATETIME)), 0)
    ) AS avg_ddd
  FROM dermtreatment.treatments
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    1
)
SELECT
  drugs.drug_name,
  _s3.avg_ddd
FROM dermtreatment.drugs AS drugs
LEFT JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM dermtreatment.treatments
    WHERE
      NOT end_dt IS NULL AND drugs.drug_id = drug_id
  )
