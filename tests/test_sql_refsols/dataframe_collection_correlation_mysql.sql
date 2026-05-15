WITH _s3 AS (
  SELECT
    classes_2.`key`,
    COUNT(*) AS n_rows
  FROM (VALUES
    ROW(15112, 'Programming Fundamentals', 'Python'),
    ROW(15122, 'Imperative Programming', 'C'),
    ROW(15150, 'Functional Programming', 'SML'),
    ROW(15210, 'Parallel Algorithms', 'SML'),
    ROW(15251, 'Theoretical CS', NULL)) AS classes_2(`key`, class_name, language)
  JOIN (VALUES
    ROW(15112, 'Programming Fundamentals', 'Python'),
    ROW(15122, 'Imperative Programming', 'C'),
    ROW(15150, 'Functional Programming', 'SML'),
    ROW(15210, 'Parallel Algorithms', 'SML'),
    ROW(15251, 'Theoretical CS', NULL)) AS classes_3(`key`, class_name, language)
    ON classes_2.`key` <> classes_3.`key` AND classes_2.language = classes_3.language
  GROUP BY
    1
)
SELECT
  classes.class_name,
  classes.language,
  COALESCE(_s3.n_rows, 0) AS n_other_classes
FROM (VALUES
  ROW(15112, 'Programming Fundamentals', 'Python'),
  ROW(15122, 'Imperative Programming', 'C'),
  ROW(15150, 'Functional Programming', 'SML'),
  ROW(15210, 'Parallel Algorithms', 'SML'),
  ROW(15251, 'Theoretical CS', NULL)) AS classes(`key`, class_name, language)
LEFT JOIN _s3 AS _s3
  ON _s3.`key` = classes.`key`
