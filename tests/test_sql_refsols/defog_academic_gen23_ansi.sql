WITH _s1 AS (
  SELECT
    oid
  FROM main.organization
)
SELECT
  author.name,
  author.aid AS author_id
FROM main.author AS author
ANTI JOIN _s1 AS _s1
  ON _s1.oid = author.oid
