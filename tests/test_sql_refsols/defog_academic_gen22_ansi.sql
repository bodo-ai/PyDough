WITH _s0 AS (
  SELECT
    aid,
    did
  FROM main.domain_author
), _s5 AS (
  SELECT
    _s0.aid
  FROM _s0 AS _s0
  JOIN _s0 AS _s1
    ON _s0.did = _s1.did
  JOIN main.author AS author
    ON LOWER(author.name) LIKE '%martin%' AND _s1.aid = author.aid
)
SELECT
  author.name,
  author.aid AS author_id
FROM main.author AS author
SEMI JOIN _s5 AS _s5
  ON _s5.aid = author.aid
