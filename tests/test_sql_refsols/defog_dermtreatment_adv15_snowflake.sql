WITH _s1 AS (
  SELECT
    drug_id,
    AVG(
      tot_drug_amt / DATEDIFF(DAY, CAST(start_dt AS DATETIME), CAST(end_dt AS DATETIME))
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
