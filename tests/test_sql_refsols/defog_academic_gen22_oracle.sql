WITH "_S0" AS (
  SELECT
    aid AS AID,
    did AS DID
  FROM MAIN.DOMAIN_AUTHOR
), "_u_0" AS (
  SELECT
    "_S0".AID AS "_u_1"
  FROM "_S0" "_S0"
  JOIN "_S0" "_S1"
    ON "_S0".DID = "_S1".DID
  JOIN MAIN.AUTHOR AUTHOR
    ON AUTHOR.aid = "_S1".AID AND LOWER(AUTHOR.name) LIKE '%martin%'
  GROUP BY
    "_S0".AID
)
SELECT
  AUTHOR.name,
  AUTHOR.aid AS author_id
FROM MAIN.AUTHOR AUTHOR
LEFT JOIN "_u_0" "_u_0"
  ON AUTHOR.aid = "_u_0"."_u_1"
WHERE
  NOT "_u_0"."_u_1" IS NULL
