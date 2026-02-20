WITH _t2 AS (
  SELECT
    lineword,
    player
  FROM shake
  WHERE
    play = 'the tempest'
  QUALIFY
    DENSE_RANK() OVER (PARTITION BY player ORDER BY act, scene, line) = 3
)
SELECT
  player,
  LISTAGG(lineword, ' ') AS third_line_words
FROM _t2
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
