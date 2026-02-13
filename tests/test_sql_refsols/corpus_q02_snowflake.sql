WITH _u_0 AS (
  SELECT
    lineword AS _u_1
  FROM shake
  GROUP BY
    1
), _t1 AS (
  SELECT
    dict.definition,
    dict.pos,
    dict.word
  FROM dict AS dict
  LEFT JOIN _u_0 AS _u_0
    ON _u_0._u_1 = dict.word
  WHERE
    NOT _u_0._u_1 IS NULL AND STARTSWITH(dict.word, 'aba')
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY dict.word ORDER BY LENGTH(dict.definition) DESC, dict.definition) = 1
)
SELECT
  word,
  pos AS part_of_speech,
  definition
FROM _t1
ORDER BY
  1 NULLS FIRST
