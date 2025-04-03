SELECT
  "table"."b" AS "b"
FROM "table" AS "table"
WHERE
  "table"."b" LIKE '%a%' AND "table"."b" LIKE (
    '%' || "table"."a" || '%'
  )
