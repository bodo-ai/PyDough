SELECT
  CAST(STRFTIME('%Y', a) AS INTEGER) AS a
FROM (
  SELECT
    a,
    b
  FROM table
) AS _t0
