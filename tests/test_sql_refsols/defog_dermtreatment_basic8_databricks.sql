SELECT
  specialty,
  COUNT(*) AS num_doctors
FROM defog.dermtreatment.doctors
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT 2
