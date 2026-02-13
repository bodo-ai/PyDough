WITH _u_0 AS (
  SELECT
    lineword AS _u_1
  FROM shake
  GROUP BY
    1
)
SELECT
  dict.word
FROM dict AS dict
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = dict.word
WHERE
  NOT _u_0._u_1 IS NULL AND STARTSWITH(dict.word, 'aba')
ORDER BY
  1 NULLS FIRST
