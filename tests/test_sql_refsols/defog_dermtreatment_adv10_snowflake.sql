WITH _s3 AS (
  SELECT
    COUNT(*) AS num_adverse_events,
    treatments.drug_id
  FROM main.treatments AS treatments
  JOIN main.adverse_events AS adverse_events
    ON DATE_TRUNC('MONTH', CAST(adverse_events.reported_dt AS TIMESTAMP)) = DATE_TRUNC('MONTH', CAST(treatments.start_dt AS TIMESTAMP))
    AND adverse_events.treatment_id = treatments.treatment_id
  GROUP BY
    2
)
SELECT
  drugs.drug_id,
  drugs.drug_name,
  _s3.num_adverse_events
FROM main.drugs AS drugs
JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
