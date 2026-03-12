SELECT
  name,
  aid AS author_id
FROM main.author
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.organization
    WHERE
      author.oid = oid
  )
