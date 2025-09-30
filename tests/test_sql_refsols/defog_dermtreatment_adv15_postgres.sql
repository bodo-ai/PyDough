WITH _s1 AS (
  SELECT
    drug_id,
    AVG(
      CAST(CAST(tot_drug_amt AS DOUBLE PRECISION) / (
        CAST(end_dt AS DATE) - CAST(start_dt AS DATE)
      ) AS DECIMAL)
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
