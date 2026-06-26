SELECT
  AVG(
    DATE_DIFF(
      'YEAR',
      CAST(date_of_birth AS TIMESTAMP),
      CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP)
    )
  ) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
