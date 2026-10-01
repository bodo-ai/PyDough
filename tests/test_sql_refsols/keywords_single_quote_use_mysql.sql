SELECT
  DESCRIPTION AS description
FROM keywords.master
WHERE
  DESCRIPTION <> 'One-One ''master row' AND ID1 = 1 AND ID2 = 1
