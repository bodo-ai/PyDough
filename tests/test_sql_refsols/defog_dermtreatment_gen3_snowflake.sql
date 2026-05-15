SELECT
  AVG(
    DATEDIFF(
      YEAR,
      CAST(date_of_birth AS DATETIME),
      CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
    )
  ) AS average_age
FROM dermtreatment.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
