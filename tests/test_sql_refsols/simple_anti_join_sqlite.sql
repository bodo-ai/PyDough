WITH _s1 AS (
  SELECT
    table.a AS a
  FROM table AS table
)
SELECT
  _s0.a AS a
FROM _s1 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.a = _s1.a
  )
