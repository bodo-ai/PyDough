SELECT
  dict.word AS verb,
  COUNT(*) AS n
FROM dict AS dict
JOIN dict AS dict_2
  ON (
    CONTAINS(LOWER(dict_2.definition), CONCAT_WS('', ' ', dict.word, ' '))
    OR ENDSWITH(LOWER(dict_2.definition), CONCAT_WS('', ' ', dict.word))
    OR STARTSWITH(LOWER(dict_2.definition), CONCAT_WS('', dict.word, ' '))
    OR dict.word = LOWER(dict_2.definition)
  )
  AND dict_2.pos = 'a.'
WHERE
  CONTAINS(dict.word, 'sh') AND dict.pos = 'v. t.'
GROUP BY
  dict.pos,
  1
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 10
