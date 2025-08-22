SELECT
  AVG(
    CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', date_of_birth) AS INTEGER)
  ) AS average_age
FROM main.patients
WHERE
  LOWER(gender) = 'male' AND LOWER(ins_type) = 'private'
