SELECT
  `quoted-name`.`name space`
FROM (VALUES
  ROW(0),
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(4)) AS `quoted-name`(`name space`)
