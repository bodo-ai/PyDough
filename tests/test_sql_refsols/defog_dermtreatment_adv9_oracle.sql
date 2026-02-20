WITH "_S2" AS (
  SELECT
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(start_dt AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(start_dt AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(start_dt AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(start_dt AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    ) AS TREATMENT_MONTH,
    COUNT(DISTINCT patient_id) AS NDISTINCT_PATIENT_ID
  FROM MAIN.TREATMENTS
  WHERE
    start_dt < TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH')
    AND start_dt >= (
      TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH') + NUMTOYMINTERVAL(3, 'month')
    )
  GROUP BY
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(start_dt AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(start_dt AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(start_dt AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(start_dt AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    )
), "_S3" AS (
  SELECT
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(TREATMENTS.start_dt AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(TREATMENTS.start_dt AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    ) AS TREATMENT_MONTH,
    COUNT(DISTINCT TREATMENTS.patient_id) AS NDISTINCT_PATIENT_ID
  FROM MAIN.TREATMENTS TREATMENTS
  JOIN MAIN.DRUGS DRUGS
    ON DRUGS.drug_id = TREATMENTS.drug_id AND DRUGS.drug_type = 'biologic'
  WHERE
    TREATMENTS.start_dt < TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH')
    AND TREATMENTS.start_dt >= (
      TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH') + NUMTOYMINTERVAL(3, 'month')
    )
  GROUP BY
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(TREATMENTS.start_dt AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(TREATMENTS.start_dt AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(TREATMENTS.start_dt AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    )
)
SELECT
  "_S2".TREATMENT_MONTH AS month,
  "_S2".NDISTINCT_PATIENT_ID AS patient_count,
  COALESCE("_S3".NDISTINCT_PATIENT_ID, 0) AS biologic_treatment_count
FROM "_S2" "_S2"
LEFT JOIN "_S3" "_S3"
  ON "_S2".TREATMENT_MONTH = "_S3".TREATMENT_MONTH
ORDER BY
  1 DESC NULLS LAST
