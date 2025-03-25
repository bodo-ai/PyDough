SELECT
  IIF(b >= 0, 'Positive', 'Negative') AS a
FROM (
  SELECT
    a,
    b
  FROM table
) AS _t0
