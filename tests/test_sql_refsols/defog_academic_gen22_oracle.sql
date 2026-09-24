WITH "_s0" AS (
  SELECT
    AID,
    DID
  FROM MAIN.DOMAIN_AUTHOR
), "_u_0" AS (
  SELECT
    "_s0".AID AS "_u_1"
  FROM "_s0" "_s0"
  JOIN "_s0" "_s1"
    ON "_s0".DID = "_s1".DID
  JOIN MAIN.AUTHOR AUTHOR
    ON AUTHOR.AID = "_s1".AID AND LOWER(AUTHOR.NAME) LIKE '%martin%'
  GROUP BY
    "_s0".AID
)
SELECT
  AUTHOR.NAME AS name,
  AUTHOR.AID AS author_id
FROM MAIN.AUTHOR AUTHOR
LEFT JOIN "_u_0" "_u_0"
  ON AUTHOR.AID = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
