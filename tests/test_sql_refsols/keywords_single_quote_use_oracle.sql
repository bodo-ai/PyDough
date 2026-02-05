SELECT
  description
FROM KEYWORDS.MASTER
WHERE
  description <> 'One-One ''master row' AND id1 = 1 AND id2 = 1
