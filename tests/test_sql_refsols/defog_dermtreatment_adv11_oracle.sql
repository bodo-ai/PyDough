SELECT
  COUNT(*) AS num_patients_with_gmail_or_yahoo
FROM MAIN.PATIENTS
WHERE
  EMAIL LIKE '%@gmail.com' OR EMAIL LIKE '%@yahoo.com'
