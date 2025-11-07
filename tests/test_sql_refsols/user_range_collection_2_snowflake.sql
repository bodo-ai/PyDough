WITH a AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 10))
), b AS (
  SELECT
    SEQ4() * 2 AS y
  FROM TABLE(GENERATOR(ROWCOUNT => 501))
), _s4 AS (
  SELECT
    a.x,
    COUNT(*) AS n_rows
  FROM a AS a
  JOIN b AS b
    ON STARTSWITH(CAST(b.y AS TEXT), CAST(a.x AS TEXT))
  GROUP BY
    1
), a_2 AS (
  SELECT
    SEQ4() AS x
  FROM TABLE(GENERATOR(ROWCOUNT => 10))
), b_2 AS (
  SELECT
    SEQ4() * 2 AS y
  FROM TABLE(GENERATOR(ROWCOUNT => 501))
), _s5 AS (
  SELECT
    a.x,
    COUNT(*) AS n_rows
  FROM a_2 AS a
  JOIN b_2 AS b
    ON ENDSWITH(CAST(b.y AS TEXT), CAST(a.x AS TEXT))
  GROUP BY
    1
)
SELECT
  _s4.x,
  _s4.n_rows AS n_prefix,
  _s5.n_rows AS n_suffix
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.x = _s5.x
ORDER BY
  1 NULLS FIRST
