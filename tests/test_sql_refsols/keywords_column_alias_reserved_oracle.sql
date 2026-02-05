SELECT
  "MixedCase_1:1"."Id" AS id_,
  "MixedCase_1:1"."LowerCaseId" AS LowerCaseID,
  "UPPERCASE_MASTER"."INTEGER" AS integer,
  "lowercase_detail"."as" AS as_,
  "UPPERCASE_MASTER"."ORDER BY" AS order_
FROM KEYWORDS."MixedCase_1:1" "MixedCase_1:1"
JOIN KEYWORDS."lowercase_detail" "lowercase_detail"
  ON "MixedCase_1:1"."LowerCaseId" = "lowercase_detail".id
  AND "lowercase_detail"."as" = '10 as reserved word'
JOIN KEYWORDS."UPPERCASE_MASTER" "UPPERCASE_MASTER"
  ON "MixedCase_1:1"."Id" = "UPPERCASE_MASTER".id
WHERE
  "MixedCase_1:1"."(parentheses)" = '5 (parentheses)'
