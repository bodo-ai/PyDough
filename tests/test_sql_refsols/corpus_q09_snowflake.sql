WITH _s0 AS (
  SELECT
    word
  FROM dict
  WHERE
    pos = 'n.'
  ORDER BY
    LENGTH(definition) DESC NULLS LAST,
    1 NULLS FIRST
  LIMIT 50
)
SELECT
  shake.play,
  COUNT(*) AS n
FROM _s0 AS _s0
JOIN shake AS shake
  ON _s0.word = shake.lineword
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
