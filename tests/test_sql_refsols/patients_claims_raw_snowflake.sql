WITH _u_0 AS (
  SELECT
    patient_id AS _u_1
  FROM bodo.health.claims
  WHERE
    PTY_UNPROTECT(claim_status, 'deAccount') = 'Denied'
    AND PTY_UNPROTECT(provider_name, 'deName') IN ('Smith Ltd', 'Smith Inc')
    AND YEAR(CAST(PTY_UNPROTECT_DOB(claim_date) AS TIMESTAMP)) = 2024
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients AS protected_patients
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = protected_patients.patient_id
WHERE
  NOT _u_0._u_1 IS NULL
