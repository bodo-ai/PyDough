SELECT
  ANY_VALUE(classes.class_name) AS class_name,
  ANY_VALUE(classes.language) AS language,
  COALESCE(COUNT_IF(classes.key <> classes_2.key), 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes(key, class_name, language)
JOIN (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes_2(key, class_name, language)
  ON classes.language = classes_2.language
GROUP BY
  classes.key
