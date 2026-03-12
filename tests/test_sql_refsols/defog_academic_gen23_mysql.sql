SELECT
  name,
  aid AS author_id
FROM author
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM organization
    WHERE
      author.oid = oid
  )
