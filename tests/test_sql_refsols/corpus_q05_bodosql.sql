SELECT
  dict.pos AS part_of_speech,
  COUNT(*) AS n_uses
FROM dict AS dict
JOIN shake AS shake
  ON dict.word = shake.lineword
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
