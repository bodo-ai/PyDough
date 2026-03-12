WITH _s1 AS (
  SELECT
    a
  FROM table
)
SELECT
  a
FROM _s1
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1
    WHERE
      _s0.a = a
  )
