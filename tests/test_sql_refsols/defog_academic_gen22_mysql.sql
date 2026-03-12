WITH _s0 AS (
  SELECT
    aid,
    did
  FROM domain_author
)
SELECT
  name,
  aid AS author_id
FROM author
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM _s0 AS _s0
    JOIN _s0 AS _s1
      ON _s0.did = _s1.did
    JOIN author AS author
      ON LOWER(author.name) LIKE '%martin%' AND _s1.aid = author.aid
    WHERE
      _s0.aid = author.aid
  )
