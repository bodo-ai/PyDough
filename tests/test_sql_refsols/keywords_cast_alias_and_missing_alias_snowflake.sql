SELECT
  "CAST".id2 AS id1,
  "CAST".id AS id2,
  "lowercase_detail"."select" AS fk1_select,
  "lowercase_detail"."as" AS fk1_as,
  lowercase_detail_2."two words" AS fk2_two_words
FROM keywords."CAST" AS "CAST"
JOIN keywords."lowercase_detail" AS "lowercase_detail"
  ON "CAST".id2 = "lowercase_detail".id
  AND "lowercase_detail"."0 = 0 and '" = '2 "0 = 0 and \'" field name'
JOIN keywords."lowercase_detail" AS lowercase_detail_2
  ON "CAST".id = lowercase_detail_2.id AND lowercase_detail_2.id = 1
WHERE
  "CAST".id = 1
