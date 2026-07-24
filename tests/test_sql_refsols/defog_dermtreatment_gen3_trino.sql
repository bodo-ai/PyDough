SELECT
  AVG(
    DATE_DIFF(
      'YEAR',
      CAST(DATE_TRUNC('YEAR', CAST(date_of_birth AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('YEAR', CURRENT_TIMESTAMP) AS TIMESTAMP)
    )
  ) AS average_age
FROM cassandra.defog.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
