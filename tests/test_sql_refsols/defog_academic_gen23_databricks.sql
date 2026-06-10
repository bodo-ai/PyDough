WITH _u_0 AS (
  SELECT
    oid AS _u_1
  FROM main.organization
  GROUP BY
    1
)
SELECT
  author.name,
  author.aid AS author_id
FROM main.author AS author
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = author.oid
WHERE
  _u_0._u_1 IS NULL
