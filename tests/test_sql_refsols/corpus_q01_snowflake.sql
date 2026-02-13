SELECT
  COUNT(*) AS n
FROM dict
WHERE
  STARTSWITH(word, 'n') AND pos = 'n.'
