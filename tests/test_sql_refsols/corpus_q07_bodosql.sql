WITH _s0 AS (
  SELECT
    lineword
  FROM shake
  WHERE
    act = 1 AND scene = 1
  ORDER BY
    LENGTH(lineword) DESC NULLS LAST,
    1 NULLS FIRST
  LIMIT 20
), _t2 AS (
  SELECT
    definition,
    word
  FROM dict
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY word ORDER BY pos) = 1
)
SELECT
  _s0.lineword AS word,
  _t2.definition
FROM _s0 AS _s0
LEFT JOIN _t2 AS _t2
  ON _s0.lineword = _t2.word
ORDER BY
  LENGTH(_s0.lineword) DESC NULLS LAST,
  1 NULLS FIRST
