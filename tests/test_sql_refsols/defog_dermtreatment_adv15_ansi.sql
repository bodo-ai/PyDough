WITH _s3 AS (
  SELECT
    drug_id,
    AVG(
      tot_drug_amt / DATEDIFF(CAST(end_dt AS DATETIME), CAST(start_dt AS DATETIME), DAY)
    ) AS avg_ddd
  FROM main.treatments
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    1
)
SELECT
  drugs.drug_name,
  _s3.avg_ddd
FROM main.drugs AS drugs
JOIN main.treatments AS treatments
  ON NOT treatments.end_dt IS NULL AND drugs.drug_id = treatments.drug_id
JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
