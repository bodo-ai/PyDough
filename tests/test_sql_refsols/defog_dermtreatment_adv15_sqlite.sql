WITH _s3 AS (
  SELECT
    drug_id,
    AVG(
      CAST(tot_drug_amt AS REAL) / NULLIF(
        CAST((
          JULIANDAY(DATE(end_dt, 'start of day')) - JULIANDAY(DATE(start_dt, 'start of day'))
        ) AS INTEGER),
        0
      )
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
LEFT JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.treatments
    WHERE
      NOT end_dt IS NULL AND drugs.drug_id = drug_id
  )
