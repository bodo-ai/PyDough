SELECT
  asian_nations_t8.nation_name,
  asian_custs_t8.ckey
FROM asian_nations_t8 AS asian_nations_t8
JOIN asian_custs_t8 AS asian_custs_t8
  ON asian_custs_t8.nkey = asian_nations_t8.nation_key
ORDER BY
  2 NULLS FIRST
LIMIT 5
