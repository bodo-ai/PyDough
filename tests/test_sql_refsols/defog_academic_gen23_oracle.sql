WITH "_u_0" AS (
  SELECT
    oid AS "_u_1"
  FROM MAIN.ORGANIZATION
  GROUP BY
    oid
)
SELECT
  AUTHOR.name,
  AUTHOR.aid AS author_id
FROM MAIN.AUTHOR AUTHOR
LEFT JOIN "_u_0" "_u_0"
  ON AUTHOR.oid = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL
