SELECT
  COUNT(*) AS num_patients_with_gmail_or_yahoo
FROM postgres.patients
WHERE
  email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com'
