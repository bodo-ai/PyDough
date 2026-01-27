SELECT
  COUNT(*) AS num_patients_with_gmail_or_yahoo
FROM dermtreatment.patients
WHERE
  ENDSWITH(email, '@gmail.com') OR ENDSWITH(email, '@yahoo.com')
