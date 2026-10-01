WITH "_s3" AS (
  SELECT
    CLASSES_2."key",
    COUNT(*) AS N_ROWS
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS CLASSES_2("key", CLASS_NAME, LANGUAGE)
  JOIN (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS CLASSES_3("key", CLASS_NAME, LANGUAGE)
    ON CLASSES_2.LANGUAGE = CLASSES_3.LANGUAGE AND CLASSES_2."key" <> CLASSES_3."key"
  GROUP BY
    CLASSES_2."key"
)
SELECT
  CLASSES.CLASS_NAME AS class_name,
  CLASSES.LANGUAGE AS language,
  COALESCE("_s3".N_ROWS, 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS CLASSES("key", CLASS_NAME, LANGUAGE)
LEFT JOIN "_s3" "_s3"
  ON CLASSES."key" = "_s3"."key"
