SELECT
  "MixedCase_1:1"."Id" AS "id_",
  "MixedCase_1:1"."LowerCaseId" AS "LowerCaseID",
  "uppercase_master"."INTEGER" AS "integer",
  "lowercase_detail"."as" AS "as_",
  "uppercase_master"."ORDER BY" AS "order_"
FROM keywords."MixedCase_1:1" AS "MixedCase_1:1"
JOIN keywords."lowercase_detail" AS "lowercase_detail"
  ON "MixedCase_1:1"."LowerCaseId" = "lowercase_detail".id
  AND "lowercase_detail"."as" = '10 as reserved word'
JOIN keywords."UPPERCASE_MASTER" AS "uppercase_master"
  ON "MixedCase_1:1"."Id" = "uppercase_master".id
WHERE
  "MixedCase_1:1"."(parentheses)" = '5 (parentheses)'
