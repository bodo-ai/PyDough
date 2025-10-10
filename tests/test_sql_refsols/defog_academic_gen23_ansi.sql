SELECT
  author.name,
  author.aid AS author_id
FROM main.author AS author
JOIN main.organization AS organization
  ON author.oid = organization.oid
