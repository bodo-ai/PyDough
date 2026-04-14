WITH _s3 AS (
  SELECT
    classes_2.column1 AS key,
    COUNT(*) AS n_rows
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS classes_2
  JOIN (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS classes_3
    ON classes_2.column1 <> classes_3.column1 AND classes_2.column3 = classes_3.column3
  GROUP BY
    1
)
SELECT
  classes.column2 AS class_name,
  classes.column3 AS language,
  COALESCE(_s3.n_rows, 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes
LEFT JOIN _s3 AS _s3
  ON _s3.key = classes.column1
