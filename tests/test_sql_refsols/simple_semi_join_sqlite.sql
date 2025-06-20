SELECT
  _s0.a AS a
FROM table AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM table AS _s1
    WHERE
      _s0.a = _s1.a
  )
