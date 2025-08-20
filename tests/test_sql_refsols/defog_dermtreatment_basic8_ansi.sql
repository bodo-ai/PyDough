SELECT
  specialty,
  COUNT(*) AS num_doctors
FROM main.doctors
GROUP BY
  specialty
ORDER BY
  num_doctors DESC
LIMIT 2
