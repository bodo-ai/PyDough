SELECT
  MAX(classes.column2) AS class_name,
  MAX(classes.column3) AS language,
  COALESCE(SUM(classes.column1 <> classes_2.column1), 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes
JOIN (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes_2
  ON classes.column3 = classes_2.column3
GROUP BY
  classes.column1
