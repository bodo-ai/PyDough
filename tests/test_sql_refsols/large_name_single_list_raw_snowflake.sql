SELECT
  COUNT(*) AS n
FROM bodo.health.protected_patients
WHERE
  PTY_UNPROTECT(last_name, 'deName') IN ('Bailey', 'Baker', 'Ball', 'Banks', 'Barajas', 'Barnett', 'Barrera', 'Barron', 'Barton', 'Cabrera', 'Calderon', 'Caldwell', 'Callahan', 'Campbell', 'Campos', 'Cardenas', 'Carey', 'Carlson', 'Carney', 'Carpenter', 'Carr', 'Carroll', 'Carter', 'Casey', 'Castillo', 'Castro', 'Daniels', 'Davenport', 'Davis', 'Dawson', 'Day', 'Eaton', 'Farrell', 'Galvan', 'Garcia', 'Garrett', 'Garrison', 'Gates', 'Hahn', 'Hale', 'Hall', 'Hampton', 'Hansen', 'Hanson', 'Hardy', 'Harris', 'Harrison', 'Harvey', 'Hatfield', 'Hawkins', 'Hayden', 'Hayes', 'Jackson', 'Jacobs', 'Jacobson', 'James', 'Kane', 'Lara', 'Larsen', 'Larson')
