SELECT
  "mixedcase_1:1".id AS id_,
  "mixedcase_1:1".lowercaseid AS LowerCaseID,
  uppercase_master.integer,
  uppercase_master."order by" AS order_
FROM keywords."mixedcase_1:1" AS "mixedcase_1:1"
JOIN keywords.lowercase_detail AS lowercase_detail
  ON "mixedcase_1:1".lowercaseid = lowercase_detail.id
  AND lowercase_detail."as" = '10 as reserved word'
JOIN keywords.uppercase_master AS uppercase_master
  ON "mixedcase_1:1".id = uppercase_master.id
WHERE
  "mixedcase_1:1"."(parentheses)" = '5 (parentheses)'
