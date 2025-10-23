WITH _s0 AS (
  SELECT
    aid,
    did
  FROM main.domain_author
), _u_0 AS (
  SELECT
    _s0.aid AS _u_1
  FROM _s0 AS _s0
  JOIN main.domain AS domain
    ON _s0.did = domain.did
  JOIN _s0 AS _s3
    ON _s3.did = domain.did
  JOIN main.author AS author
    ON LOWER(author.name) LIKE '%martin%' AND _s3.aid = author.aid
  GROUP BY
    1
)
SELECT
  author.name,
  author.aid AS author_id
FROM main.author AS author
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = author.aid
WHERE
  NOT _u_0._u_1 IS NULL
