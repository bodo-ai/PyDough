WITH "_S3" AS (
  SELECT
    CLASSES_2."KEY",
    COUNT(*) AS N_ROWS
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS CLASSES_2("KEY", CLASS_NAME, LANGUAGE)
  JOIN (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS CLASSES_3("KEY", CLASS_NAME, LANGUAGE)
    ON CLASSES_2."KEY" <> CLASSES_3."KEY" AND CLASSES_2.LANGUAGE = CLASSES_3.LANGUAGE
  GROUP BY
    CLASSES_2."KEY"
)
SELECT
  CLASSES.CLASS_NAME AS class_name,
  CLASSES.LANGUAGE AS language,
  COALESCE("_S3".N_ROWS, 0) AS n_other_classes
FROM (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS CLASSES("KEY", CLASS_NAME, LANGUAGE)
LEFT JOIN "_S3" "_S3"
  ON CLASSES."KEY" = "_S3"."KEY"
