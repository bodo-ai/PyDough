SELECT
  AVG(
    CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', date_of_birth) AS INTEGER)
  ) AS average_age
FROM main.patients
WHERE
  gender = 'Male' AND ins_type = 'private'
