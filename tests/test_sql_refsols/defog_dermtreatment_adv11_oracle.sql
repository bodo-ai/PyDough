SELECT
  COUNT(*) AS num_patients_with_gmail_or_yahoo
FROM MAIN.PATIENTS
WHERE
  email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com'
