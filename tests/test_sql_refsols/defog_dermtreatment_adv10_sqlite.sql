WITH _s3 AS (
  SELECT
    treatments.drug_id,
    COUNT(*) AS n_rows
  FROM main.treatments AS treatments
  JOIN main.adverse_events AS adverse_events
    ON DATE(adverse_events.reported_dt, 'start of month') = DATE(treatments.start_dt, 'start of month')
    AND adverse_events.treatment_id = treatments.treatment_id
  GROUP BY
    1
)
SELECT
  drugs.drug_id,
  drugs.drug_name,
  _s3.n_rows AS num_adverse_events
FROM main.drugs AS drugs
JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
ORDER BY
  3 DESC
LIMIT 1
