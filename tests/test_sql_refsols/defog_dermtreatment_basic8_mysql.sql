SELECT
  specialty,
  COUNT(*) AS num_doctors
FROM main.doctors
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 2
