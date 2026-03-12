SELECT
  name,
  aid AS author_id
FROM academic.author
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM academic.organization
    WHERE
      author.oid = oid
  )
