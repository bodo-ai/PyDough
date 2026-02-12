WITH _t AS (
  SELECT
    teaching.class_key,
    teachers.first_name,
    teachers.last_name,
    teaching.rating,
    ROW_NUMBER() OVER (PARTITION BY teachers.tid ORDER BY CASE WHEN teaching.rating IS NULL THEN 1 ELSE 0 END DESC, teaching.rating DESC) AS _w
  FROM (VALUES
    ROW(1, 'Anil', 'Lee'),
    ROW(2, 'Mike', 'Lee'),
    ROW(3, 'Ian', 'Lee'),
    ROW(4, 'David', 'Smith'),
    ROW(5, 'Anil', 'Smith'),
    ROW(6, 'Mike', 'Smith'),
    ROW(7, 'Ian', 'Taylor'),
    ROW(8, 'David', 'Taylor'),
    ROW(9, 'Anil', 'Taylor'),
    ROW(10, 'Mike', 'Thomas'),
    ROW(11, 'Ian', 'Thomas'),
    ROW(12, 'David', 'Thomas')) AS teachers(tid, first_name, last_name)
  JOIN (VALUES
    ROW(15112, 1, '2020-09-01', 11.39),
    ROW(15122, 2, '2020-09-01', 9.22),
    ROW(15150, 9, '2020-09-01', 11.93),
    ROW(15210, 4, '2020-09-01', 0.32),
    ROW(15251, 5, '2020-09-01', 3.19),
    ROW(15112, 6, '2021-02-01', 1.35),
    ROW(15122, 1, '2021-02-01', 11.58),
    ROW(15150, 8, '2021-02-01', 2.69),
    ROW(15210, 9, '2021-02-01', 3.48),
    ROW(15251, 10, '2021-02-01', 6.75),
    ROW(15112, 5, '2021-09-01', 5.31),
    ROW(15122, 12, '2021-09-01', 3.94),
    ROW(15150, 1, '2021-09-01', 7.45),
    ROW(15210, 2, '2021-09-01', 8.64),
    ROW(15251, 9, '2021-09-01', 0.31),
    ROW(15112, 4, '2022-02-01', 11.27),
    ROW(15122, 5, '2022-02-01', 10.3),
    ROW(15150, 6, '2022-02-01', 2.21),
    ROW(15210, 1, '2022-02-01', 3.8),
    ROW(15251, 8, '2022-02-01', 7.87),
    ROW(15112, 9, '2022-09-01', 7.23),
    ROW(15122, 10, '2022-09-01', 6.66),
    ROW(15150, 5, '2022-09-01', 10.97),
    ROW(15210, 12, '2022-09-01', 0.96),
    ROW(15251, 1, '2022-09-01', 5.43),
    ROW(15112, 2, '2023-02-01', 5.19),
    ROW(15122, 9, '2023-02-01', 5.02),
    ROW(15150, 4, '2023-02-01', 9.73),
    ROW(15210, 5, '2023-02-01', 0.12),
    ROW(15251, 6, '2023-02-01', 4.99)) AS teaching(class_key, teacher_id, semester, rating)
    ON teachers.tid = teaching.teacher_id
)
SELECT
  _t.first_name,
  _t.last_name,
  _t.rating,
  classes.class_name
FROM _t AS _t
JOIN (VALUES
  ROW(15112, 'Programming Fundamentals', 'Python'),
  ROW(15122, 'Imperative Programming', 'C'),
  ROW(15150, 'Functional Programming', 'SML'),
  ROW(15210, 'Parallel Algorithms', 'SML'),
  ROW(15251, 'Theoretical CS', NULL)) AS classes(`key`, class_name, language)
  ON _t.class_key = classes.`key`
WHERE
  _t._w = 1
