WITH _u_0 AS (
  SELECT
    drug_id AS _u_1
  FROM main.treatments
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    1
), _s3 AS (
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
  _s3.avg_ddd
FROM main.drugs AS drugs
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = drugs.drug_id
JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
WHERE
  NOT _u_0._u_1 IS NULL
