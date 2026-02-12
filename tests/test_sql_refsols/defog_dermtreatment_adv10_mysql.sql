WITH _s3 AS (
  SELECT
    treatments.drug_id,
    COUNT(*) AS n_rows
  FROM treatments AS treatments
  JOIN adverse_events AS adverse_events
    ON STR_TO_DATE(
      CONCAT(
        YEAR(CAST(adverse_events.reported_dt AS DATETIME)),
        ' ',
        MONTH(CAST(adverse_events.reported_dt AS DATETIME)),
        ' 1'
      ),
      '%Y %c %e'
    ) = STR_TO_DATE(
      CONCAT(
        YEAR(CAST(treatments.start_dt AS DATETIME)),
        ' ',
        MONTH(CAST(treatments.start_dt AS DATETIME)),
        ' 1'
      ),
      '%Y %c %e'
    )
    AND adverse_events.treatment_id = treatments.treatment_id
  GROUP BY
    1
)
SELECT
  drugs.drug_id,
  drugs.drug_name,
  _s3.n_rows AS num_adverse_events
FROM drugs AS drugs
JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
ORDER BY
  3 DESC
LIMIT 1
