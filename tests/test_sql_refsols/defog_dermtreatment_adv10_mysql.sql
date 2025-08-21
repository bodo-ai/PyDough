SELECT
  treatments.drug_id,
  drugs.drug_name,
  COUNT(*) AS num_adverse_events
FROM main.adverse_events AS adverse_events
JOIN main.treatments AS treatments
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
JOIN main.drugs AS drugs
  ON drugs.drug_id = treatments.drug_id
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 1
