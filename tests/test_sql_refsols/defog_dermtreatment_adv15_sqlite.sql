WITH _s1 AS (
  SELECT
    drug_id,
    AVG(
      CAST(tot_drug_amt AS REAL) / CAST((
        JULIANDAY(DATE(end_dt, 'start of day')) - JULIANDAY(DATE(start_dt, 'start of day'))
      ) AS INTEGER)
    ) AS avg_ddd
  FROM main.treatments
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    1
)
SELECT
  drugs.drug_name,
  _s1.avg_ddd
FROM main.drugs AS drugs
JOIN _s1 AS _s1
  ON _s1.drug_id = drugs.drug_id
