SELECT
  word,
  pos AS part_of_speech,
  ccount AS length,
  definition
FROM dict
WHERE
  ccount > 25
