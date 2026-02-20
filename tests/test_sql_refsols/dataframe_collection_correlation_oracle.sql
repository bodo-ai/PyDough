WITH "_S1" AS (
  SELECT
    COLUMN1 AS KEY,
    COLUMN3 AS LANGUAGE
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS CLASSES_3(KEY, CLASS_NAME, LANGUAGE)
), "_S3" AS (
  SELECT
    COLUMN1 AS KEY,
    COUNT(*) AS N_ROWS
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS CLASSES_2(KEY, CLASS_NAME, LANGUAGE)
  JOIN "_S1" "_S1"
    ON COLUMN1 <> "_S1".KEY AND COLUMN3 = "_S1".LANGUAGE
  GROUP BY
    COLUMN1
)
SELECT
  COLUMN2 AS class_name,
  COLUMN3 AS language,
  COALESCE("_S3".N_ROWS, 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS CLASSES(KEY, CLASS_NAME, LANGUAGE)
LEFT JOIN "_S3" "_S3"
  ON COLUMN1 = "_S3".KEY
