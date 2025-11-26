WITH _s0 AS (
  SELECT
    aid,
    did
  FROM main.domain_author
)
SELECT
  author.name,
  author.aid AS author_id
FROM main.author AS author
JOIN _s0 AS _s0
  ON _s0.aid = author.aid
JOIN _s0 AS _s1
  ON _s0.did = _s1.did
JOIN main.author AS author_2
  ON LOWER(author_2.name) LIKE '%martin%' AND _s1.aid = author_2.aid
