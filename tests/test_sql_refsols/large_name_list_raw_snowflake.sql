SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  PTY_UNPROTECT(last_name, 'deName') IN ('Bailey', 'Baker', 'Ball', 'Banks', 'Barajas', 'Barnett')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Barrera', 'Barron', 'Barton', 'Cabrera', 'Calderon')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Caldwell', 'Callahan', 'Campbell', 'Campos', 'Cardenas')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Carey', 'Carlson', 'Carney', 'Carpenter', 'Carr')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Carroll', 'Carter', 'Casey', 'Castillo', 'Castro')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Daniels', 'Davenport', 'Davis', 'Dawson', 'Day')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Eaton', 'Farrell', 'Galvan', 'Garcia', 'Garrett')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Garrison', 'Gates', 'Hahn', 'Hale', 'Hall', 'Hampton')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Hansen', 'Hanson', 'Hardy', 'Harris', 'Harrison')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Harvey', 'Hatfield', 'Hawkins', 'Hayden', 'Hayes')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Jackson', 'Jacobs', 'Jacobson', 'James')
  OR PTY_UNPROTECT(last_name, 'deName') IN ('Kane', 'Lara', 'Larsen', 'Larson')
