WITH _s1 AS (
  SELECT
    column1 AS key,
    column3 AS language
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS classes_3(key, class_name, language)
), _s3 AS (
  SELECT
    column1 AS key,
    COUNT(*) AS n_rows
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS classes_2(key, class_name, language)
  JOIN _s1 AS _s1
    ON _s1.key <> column1 AND _s1.language = column3
  GROUP BY
    1
)
SELECT
  column2 AS class_name,
  column3 AS language,
  COALESCE(_s3.n_rows, 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes(key, class_name, language)
LEFT JOIN _s3 AS _s3
  ON _s3.key = column1
