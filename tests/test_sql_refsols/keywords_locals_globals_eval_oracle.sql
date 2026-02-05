SELECT
  "COUNT".node,
  "CAST".types,
  2.0 AS expr
FROM KEYWORDS."COUNT" "COUNT"
JOIN KEYWORDS."CAST" "CAST"
  ON "CAST".pk_field_name = "COUNT".this
WHERE
  "COUNT".node = 4071
