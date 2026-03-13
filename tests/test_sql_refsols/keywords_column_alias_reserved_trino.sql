SELECT
  "mixedcase_1:1"."id" AS id_,
  "mixedcase_1:1"."lowercaseid" AS LowerCaseID,
  "uppercase_master"."integer",
  "lowercase_detail"."as" AS as_,
  "uppercase_master"."order by" AS order_
FROM keywords."mixedcase_1:1" AS "mixedcase_1:1"
JOIN keywords."lowercase_detail" AS "lowercase_detail"
  ON "lowercase_detail"."as" = '10 as reserved word'
  AND "lowercase_detail".id = "mixedcase_1:1"."lowercaseid"
JOIN keywords."uppercase_master" AS "uppercase_master"
  ON "mixedcase_1:1"."id" = "uppercase_master".id
WHERE
  "mixedcase_1:1"."(parentheses)" = '5 (parentheses)'
