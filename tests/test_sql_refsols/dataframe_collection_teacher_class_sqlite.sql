WITH _t AS (
  SELECT
    teaching.column1 AS class_key,
    teachers.column2 AS first_name,
    teachers.column3 AS last_name,
    teaching.column3 AS semester,
    ROW_NUMBER() OVER (PARTITION BY teachers.column1 ORDER BY teaching.column3 DESC) AS _w
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
    (12, 'David', 'Thomas')) AS teachers
  JOIN (VALUES
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
    (15251, 6, '2023-02-01', 4.99)) AS teaching
    ON teachers.column1 = teaching.column2
)
SELECT
  _t.first_name,
  _t.last_name,
  _t.semester AS recent_semester,
  classes.column2 AS class_name
FROM _t AS _t
JOIN (VALUES
  (15112, 'Programming Fundamentals', 'Python'),
  (15122, 'Imperative Programming', 'C'),
  (15150, 'Functional Programming', 'SML'),
  (15210, 'Parallel Algorithms', 'SML'),
  (15251, 'Theoretical CS', NULL)) AS classes
  ON _t.class_key = classes.column1
WHERE
  _t._w = 1
