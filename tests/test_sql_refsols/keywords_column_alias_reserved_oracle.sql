SELECT
  "MixedCase_1:1"."Id" AS "id_",
  "MixedCase_1:1"."LowerCaseId" AS "LowerCaseID",
  "UPPERCASE_MASTER"."INTEGER" AS "integer",
  "LOWERCASE_DETAIL"."as" AS "as_",
  "UPPERCASE_MASTER"."ORDER BY" AS "order_"
FROM KEYWORDS."MixedCase_1:1" "MixedCase_1:1"
JOIN KEYWORDS."lowercase_detail" "LOWERCASE_DETAIL"
  ON "LOWERCASE_DETAIL"."as" = '10 as reserved word'
  AND "LOWERCASE_DETAIL".ID = "MixedCase_1:1"."LowerCaseId"
JOIN KEYWORDS."UPPERCASE_MASTER" "UPPERCASE_MASTER"
  ON "MixedCase_1:1"."Id" = "UPPERCASE_MASTER".ID
WHERE
  "MixedCase_1:1"."(parentheses)" = '5 (parentheses)'
