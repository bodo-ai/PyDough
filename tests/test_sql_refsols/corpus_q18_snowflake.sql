SELECT
  COUNT(*) AS n
FROM shake AS shake
JOIN dict AS dict
  ON dict.ccount = shake.act AND dict.pos = 'n.' AND dict.word = shake.lineword
WHERE
  shake.act = LENGTH(shake.lineword) AND shake.play = 'cymbeline'
