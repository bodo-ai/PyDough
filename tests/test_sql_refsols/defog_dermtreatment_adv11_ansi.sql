SELECT
  COUNT(*) AS num_patients_with_gmail_or_yahoo
FROM main.patients
WHERE
  email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com'
