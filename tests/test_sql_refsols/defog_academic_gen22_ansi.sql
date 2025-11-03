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
JOIN main.domain AS domain
  ON _s0.did = domain.did
JOIN _s0 AS _s3
  ON _s3.did = domain.did
JOIN main.author AS author_2
  ON LOWER(author_2.name) LIKE '%martin%' AND _s3.aid = author_2.aid
