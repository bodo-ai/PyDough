SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  last_name IN (PTY_PROTECT('Bailey', 'deName'), PTY_PROTECT('Baker', 'deName'), PTY_PROTECT('Ball', 'deName'), PTY_PROTECT('Banks', 'deName'), PTY_PROTECT('Barajas', 'deName'), PTY_PROTECT('Barnett', 'deName'), PTY_PROTECT('Barrera', 'deName'), PTY_PROTECT('Barron', 'deName'), PTY_PROTECT('Barton', 'deName'), PTY_PROTECT('Cabrera', 'deName'))
