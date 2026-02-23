SELECT DISTINCT
  teachers.first_name,
  teachers.last_name
FROM (VALUES
  ROW(1, 'Anil', 'Lee'),
  ROW(2, 'Mike', 'Lee'),
  ROW(3, 'Ian', 'Lee'),
  ROW(4, 'David', 'Smith'),
  ROW(5, 'Anil', 'Smith'),
  ROW(6, 'Mike', 'Smith'),
  ROW(7, 'Ian', 'Taylor'),
  ROW(8, 'David', 'Taylor'),
  ROW(9, 'Anil', 'Taylor'),
  ROW(10, 'Mike', 'Thomas'),
  ROW(11, 'Ian', 'Thomas'),
  ROW(12, 'David', 'Thomas')) AS teachers(tid, first_name, last_name)
