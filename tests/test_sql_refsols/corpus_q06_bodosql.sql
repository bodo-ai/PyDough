SELECT
  shake.lineword AS word,
  shake.play,
  shake.act,
  shake.scene,
  shake.line,
  shake.player
FROM dict AS dict
JOIN shake AS shake
  ON dict.word = shake.lineword
  AND shake.play IN ('much ado about nothing', 'henry viii', 'merry wives of windsor', 'romeo and juliet', 'pericles', 'king john', 'othello')
WHERE
  LENGTH(dict.word) = 10 AND dict.pos = 'a.' AND dict.word < 'c'
