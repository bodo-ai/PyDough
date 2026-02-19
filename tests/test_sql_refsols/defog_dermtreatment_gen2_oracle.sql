WITH "_u_0" AS (
  SELECT
    ARRAY_AGG(doc_id) AS DOC_ID,
    year_reg AS "_u_1"
  FROM MAIN.DOCTORS
  GROUP BY
    year_reg
), "_T" AS (
  SELECT
    doc_id AS DOC_ID,
    start_dt AS START_DT,
    treatment_id AS TREATMENT_ID,
    ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY start_dt) AS "_W"
  FROM MAIN.TREATMENTS
), "_S1" AS (
  SELECT
    DOC_ID,
    START_DT,
    TREATMENT_ID
  FROM "_T"
  WHERE
    "_W" = 1
)
SELECT
  "_S0".LAST_NAME AS last_name,
  "_S0".YEAR_REG AS year_reg,
  "_S1".START_DT AS first_treatment_date,
  "_S1".TREATMENT_ID AS first_treatment_id
FROM "_u_0".DOC_ID
LEFT JOIN "_u_0" "_u_0"
  ON "_u_0"."_u_1" = EXTRACT(YEAR FROM CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTOYMINTERVAL(2, 'year') AS DATE))
LEFT JOIN "_S1" "_S1"
  ON "_S0".DOC_ID = "_S1".DOC_ID
