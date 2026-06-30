WITH _s3 AS (
  SELECT
    treatments.drug_id,
    COUNT(*) AS n_rows
  FROM defog.dermtreatment.treatments AS treatments
  JOIN defog.dermtreatment.adverse_events AS adverse_events
    ON TRUNC(CAST(adverse_events.reported_dt AS TIMESTAMP), 'MONTH') = TRUNC(CAST(treatments.start_dt AS TIMESTAMP), 'MONTH')
    AND adverse_events.treatment_id = treatments.treatment_id
  GROUP BY
    1
)
SELECT
  drugs.drug_id,
  drugs.drug_name,
  _s3.n_rows AS num_adverse_events
FROM defog.dermtreatment.drugs AS drugs
JOIN _s3 AS _s3
  ON _s3.drug_id = drugs.drug_id
ORDER BY
  3 DESC
LIMIT 1
