WITH _s1 AS (
  SELECT
    column1 AS class_key,
    column2 AS teacher_id,
    column4 AS rating
  FROM (VALUES
    (15112, 1, '2020-09-01', 11.39),
    (15122, 2, '2020-09-01', 9.22),
    (15150, 9, '2020-09-01', 11.93),
    (15210, 4, '2020-09-01', 0.32),
    (15251, 5, '2020-09-01', 3.19),
    (15112, 6, '2021-02-01', 1.35),
    (15122, 1, '2021-02-01', 11.58),
    (15150, 8, '2021-02-01', 2.69),
    (15210, 9, '2021-02-01', 3.48),
    (15251, 10, '2021-02-01', 6.75),
    (15112, 5, '2021-09-01', 5.31),
    (15122, 12, '2021-09-01', 3.94),
    (15150, 1, '2021-09-01', 7.45),
    (15210, 2, '2021-09-01', 8.64),
    (15251, 9, '2021-09-01', 0.31),
    (15112, 4, '2022-02-01', 11.27),
    (15122, 5, '2022-02-01', 10.3),
    (15150, 6, '2022-02-01', 2.21),
    (15210, 1, '2022-02-01', 3.8),
    (15251, 8, '2022-02-01', 7.87),
    (15112, 9, '2022-09-01', 7.23),
    (15122, 10, '2022-09-01', 6.66),
    (15150, 5, '2022-09-01', 10.97),
    (15210, 12, '2022-09-01', 0.96),
    (15251, 1, '2022-09-01', 5.43),
    (15112, 2, '2023-02-01', 5.19),
    (15122, 9, '2023-02-01', 5.02),
    (15150, 4, '2023-02-01', 9.73),
    (15210, 5, '2023-02-01', 0.12),
    (15251, 6, '2023-02-01', 4.99)) AS teaching(class_key, teacher_id, semester, rating)
), _t0 AS (
  SELECT
    _s1.class_key,
    column2 AS first_name,
    column3 AS last_name,
    _s1.rating
  FROM (VALUES
    (1, 'Anil', 'Lee'),
    (2, 'Mike', 'Lee'),
    (3, 'Ian', 'Lee'),
    (4, 'David', 'Smith'),
    (5, 'Anil', 'Smith'),
    (6, 'Mike', 'Smith'),
    (7, 'Ian', 'Taylor'),
    (8, 'David', 'Taylor'),
    (9, 'Anil', 'Taylor'),
    (10, 'Mike', 'Thomas'),
    (11, 'Ian', 'Thomas'),
    (12, 'David', 'Thomas')) AS teachers(tid, first_name, last_name)
  JOIN _s1 AS _s1
    ON _s1.teacher_id = column1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY _s1.rating DESC NULLS FIRST) = 1
), _s3 AS (
  SELECT
    column1 AS key,
    column2 AS class_name
  FROM (VALUES
    (15112, 'Programming Fundamentals', 'Python'),
    (15122, 'Imperative Programming', 'C'),
    (15150, 'Functional Programming', 'SML'),
    (15210, 'Parallel Algorithms', 'SML'),
    (15251, 'Theoretical CS', NULL)) AS classes(key, class_name, language)
)
SELECT
  _t0.first_name,
  _t0.last_name,
  _t0.rating,
  _s3.class_name
FROM _t0 AS _t0
LEFT JOIN _s3 AS _s3
  ON _s3.key = _t0.class_key
