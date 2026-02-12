SELECT
  LOWER(SPLIT_PART(colorname, ' ', 1)) AS rainbow_color,
  COUNT(*) AS n
FROM clrs
WHERE
  LOWER(SPLIT_PART(colorname, ' ', 1)) IN ('red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet')
GROUP BY
  1
