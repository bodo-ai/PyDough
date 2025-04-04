WITH _t1 AS (
  SELECT
    a AS a
  FROM table
)
SELECT
  _t0.a AS a
FROM _t1 AS _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      a = a
  )
