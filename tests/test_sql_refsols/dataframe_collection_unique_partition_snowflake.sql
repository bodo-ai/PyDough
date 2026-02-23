SELECT DISTINCT
  teachers.first_name,
  teachers.last_name
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
  (12, 'David', 'Thomas')) AS teachers(tid, first_name, last_name)
