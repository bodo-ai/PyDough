SELECT
  identname AS key,
  colorname AS name,
  chex AS hex_code,
  r AS red,
  g AS green,
  b AS blue
FROM clrs
WHERE
  ENDSWITH(colorname, 'yz')
