SELECT
  "mixedcase_1:1".id AS id_,
  "mixedcase_1:1".lowercaseid AS LowerCaseID,
  "_s3.""integer""" AS integer,
  "_s2.`as`" AS as_,
  "_s3.`order by`" AS order
FROM keywords."mixedcase_1:1" AS "mixedcase_1:1"
JOIN keywords.lowercase_detail AS lowercase_detail
  ON "mixedcase_1:1".lowercaseid = lowercase_detail.id
  AND lowercase_detail."`as`" = '10 as reserved word'
JOIN keywords.uppercase_master AS uppercase_master
  ON "mixedcase_1:1".id = uppercase_master.id
WHERE
  "mixedcase_1:1"."`(parentheses)`" = '5 (parentheses)'
