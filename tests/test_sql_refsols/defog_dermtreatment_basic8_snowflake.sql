SELECT
  specialty,
  COUNT(*) AS num_doctors
FROM dermtreatment.doctors
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 2
