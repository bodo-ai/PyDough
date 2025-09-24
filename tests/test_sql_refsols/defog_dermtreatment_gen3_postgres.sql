SELECT
  AVG(
    CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP) - EXTRACT(YEAR FROM CAST(date_of_birth AS TIMESTAMP)) AS DECIMAL)
  ) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
