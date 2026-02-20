SELECT DISTINCT
  COLUMN2 AS first_name,
  COLUMN3 AS last_name
FROM (VALUES
  (1, 'Anil', 'Lee'),
  (2, 'Mike', 'Lee'),
  (3, 'Ian', 'Lee'),
  (4, 'David', 'Smith'),
  (5, 'Anil', 'Smith'),
  (6, 'Mike', 'Smith'),
  (7, 'Ian', 'Taylor'),
  (8, 'David', 'Taylor'),
  (9, 'Anil', 'Taylor'),
  (10, 'Mike', 'Thomas'),
  (11, 'Ian', 'Thomas'),
  (12, 'David', 'Thomas')) AS TEACHERS(TID, FIRST_NAME, LAST_NAME)
