WITH _s1 AS (
  SELECT
    column1 AS key,
    column3 AS language
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS classes(key, class_name, language)
)
SELECT
  ANY_VALUE(column2) AS class_name,
  ANY_VALUE(column3) AS language,
  COALESCE(SUM(column1 <> _s1.key), 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes(key, class_name, language)
JOIN _s1 AS _s1
  ON _s1.language = column3
GROUP BY
  column1
