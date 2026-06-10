SELECT
  description
FROM keywords.master
WHERE
  description <> 'One-One \'master row' AND id1 = 1 AND id2 = 1
