SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  last_name IN (PTY_PROTECT('Bailey', 'deName'), PTY_PROTECT('Baker', 'deName'), PTY_PROTECT('Ball', 'deName'), PTY_PROTECT('Banks', 'deName'), PTY_PROTECT('Barajas', 'deName'), PTY_PROTECT('Barnett', 'deName'))
  OR last_name IN (PTY_PROTECT('Barrera', 'deName'), PTY_PROTECT('Barron', 'deName'), PTY_PROTECT('Barton', 'deName'), PTY_PROTECT('Cabrera', 'deName'), PTY_PROTECT('Calderon', 'deName'))
  OR last_name IN (PTY_PROTECT('Caldwell', 'deName'), PTY_PROTECT('Callahan', 'deName'), PTY_PROTECT('Campbell', 'deName'), PTY_PROTECT('Campos', 'deName'), PTY_PROTECT('Cardenas', 'deName'))
  OR last_name IN (PTY_PROTECT('Carey', 'deName'), PTY_PROTECT('Carlson', 'deName'), PTY_PROTECT('Carney', 'deName'), PTY_PROTECT('Carpenter', 'deName'), PTY_PROTECT('Carr', 'deName'))
  OR last_name IN (PTY_PROTECT('Carroll', 'deName'), PTY_PROTECT('Carter', 'deName'), PTY_PROTECT('Casey', 'deName'), PTY_PROTECT('Castillo', 'deName'), PTY_PROTECT('Castro', 'deName'))
  OR last_name IN (PTY_PROTECT('Daniels', 'deName'), PTY_PROTECT('Davenport', 'deName'), PTY_PROTECT('Davis', 'deName'), PTY_PROTECT('Dawson', 'deName'), PTY_PROTECT('Day', 'deName'))
  OR last_name IN (PTY_PROTECT('Eaton', 'deName'), PTY_PROTECT('Farrell', 'deName'), PTY_PROTECT('Galvan', 'deName'), PTY_PROTECT('Garcia', 'deName'), PTY_PROTECT('Garrett', 'deName'))
  OR last_name IN (PTY_PROTECT('Garrison', 'deName'), PTY_PROTECT('Gates', 'deName'), PTY_PROTECT('Hahn', 'deName'), PTY_PROTECT('Hale', 'deName'), PTY_PROTECT('Hall', 'deName'), PTY_PROTECT('Hampton', 'deName'))
  OR last_name IN (PTY_PROTECT('Hansen', 'deName'), PTY_PROTECT('Hanson', 'deName'), PTY_PROTECT('Hardy', 'deName'), PTY_PROTECT('Harris', 'deName'), PTY_PROTECT('Harrison', 'deName'))
  OR last_name IN (PTY_PROTECT('Harvey', 'deName'), PTY_PROTECT('Hatfield', 'deName'), PTY_PROTECT('Hawkins', 'deName'), PTY_PROTECT('Hayden', 'deName'), PTY_PROTECT('Hayes', 'deName'))
  OR last_name IN (PTY_PROTECT('Jackson', 'deName'), PTY_PROTECT('Jacobs', 'deName'), PTY_PROTECT('Jacobson', 'deName'), PTY_PROTECT('James', 'deName'))
  OR last_name IN (PTY_PROTECT('Kane', 'deName'), PTY_PROTECT('Lara', 'deName'), PTY_PROTECT('Larsen', 'deName'), PTY_PROTECT('Larson', 'deName'))
