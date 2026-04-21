WITH _u_0 AS (
  SELECT
    drug_id AS _u_1
  FROM cassandra.defog.treatments
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    1
), _s3 AS (
  SELECT
    drug_id,
    AVG(
      CAST(tot_drug_amt AS DOUBLE) / NULLIF(
        DATE_DIFF(
          'DAY',
          CAST(DATE_TRUNC('DAY', CAST(start_dt AS TIMESTAMP)) AS TIMESTAMP),
          CAST(DATE_TRUNC('DAY', CAST(end_dt AS TIMESTAMP)) AS TIMESTAMP)
        ),
        0
      )
    ) AS avg_ddd
  FROM cassandra.defog.treatments
  WHERE
    NOT end_dt IS NULL
  GROUP BY
    1
)
SELECT
  drugs.drug_name,
  _s3.avg_ddd
FROM postgres.main.drugs AS drugs
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = drugs.drug_id
LEFT JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
WHERE
  NOT _u_0._u_1 IS NULL
