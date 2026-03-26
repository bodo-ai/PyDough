SELECT
  asian_nations_t7.nation_name,
  asian_custs_t7.ckey
FROM asian_nations_t7 asian_nations_t7
JOIN asian_custs_t7 asian_custs_t7
  ON asian_custs_t7.nkey = asian_nations_t7.nation_key
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
